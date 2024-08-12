import json
import requests
import os

MAX_RETRIES = 3  # Max retries
RETRY_DELAY = 20  # Delay per each retry

def lambda_handler(event, context):
    
    records = event["Records"]
    
    if len(records) == 0:
        return
        
    for record in records:
        process_queue(record)
        
    return

def process_queue(record):
    body_str = record["body"].strip('"')
    body_str = body_str.replace('\\r\\n', '')
    body_str = body_str.replace('\\"', '"')
    
    try:
        body = json.loads(body_str)
    except json.JSONDecodeError as e:
        print(f"Error al decodificar JSON: {e}")
    
    section_list = []
    project_id   = body["projectId"]
    asset_parent = body["asset_parent"]
    sections     = body["sections"]
    token        = body["token"]
    

    for section in sections:
        section_list.append({ "name": section["name"], "asset_id": section["asset_id"] })
        
    process_project(project_id, asset_parent, section_list, token)
    
def process_project(project_id, asset_parent, section_list, token):
    project = get_project_by_id(project_id, token)
    
    if not project:
        restore_project(project_id, asset_parent, token)
        return
    
    # guide_runing: true
    
    project_information   = project["information"]
    project_serializers   = project["serializer_info"]
    project_urls          = project["url_info"]
    project_views         = project["view_info"]
    project_lang          = project["lang"]
    project_guide_running = project["guide_running"]
    
    if project_guide_running == True:
        return
    
    # start guide running
    updated_project = update_project_guide_running(project_id, project_guide_running, token)
    if updated_project == False:
        restore_project(project_id, asset_parent, token)
        return
    
    print("GuIA: Start resume info")
    # Generate resume
    resume_information = generate_resume(project_information, project_lang, 'project_information')
    resume_serializers = generate_resume(project_serializers, project_lang, 'project_serializers')
    resume_urls        = generate_resume(project_urls, project_lang, 'project_urls')
    resume_views       = generate_resume(project_views, project_lang, 'project_views')
    print("GuIA: End resume info")
    
    if (resume_information is None) or (resume_serializers is None) or (resume_urls is None) or (resume_views is None):
        restore_project(project_id, asset_parent, token)
        return
    
    # Generate custom general prompt
    general_prompt = f"{resume_information} \n {resume_serializers} \n {resume_urls} \n {resume_views}"
    
    # Generate guide per section
    generate_guide(project_id, asset_parent, section_list, general_prompt, project_lang, token)
        
def get_project_by_id(project_id, token):
    headers={ "Authorization": f"Bearer {token}" }
    
    api_codeia = os.getenv('API_CODEIA')
    response = requests.get(f'{api_codeia}project/info/{project_id}/', headers=headers)
    
    if response.status_code == 200:
        print(f'GuIA: Find sucess project_id: {project_id}')
        content = response.json()
        return content
    else:
        print(f'GuIA: Find failed project_id: {project_id}')
        print(response.text)
        return None

def generate_resume(content, lang, subsection):
    
    resume_prompt = os.getenv('PROMP_GUIA_RESUME_ENG') if lang == "English" else os.getenv('PROMP_GUIA_RESUME_ESP')
    prompt = f"{resume_prompt} \n {content}"
    
    print(f'GuIA: Generate resume by: {subsection}')
    
    response = call_api_guia_with_retry(prompt, lang)
    
    return response

def generate_guide(project_id, asset_parent, section_list, content, lang, token):
    
    for i, section in enumerate(section_list):
        print(f'GuIA: Start generate guide by secction: {section["name"]}')
        result = generate_guide_per_section(content, section["name"], lang)
        print(f'GuIA: End generate guide by secction: {section["name"]}')
        is_last = i == len(section_list) - 1
        
        if not result:
            print(f'GuIA: Updated failed by section: {section["name"]}')
            update_guide(project_id, asset_parent, section["asset_id"], result, False, is_last, token)
        else:
            print(f'GuIA: Updated success by section: {section["name"]}')
            update_guide(project_id, asset_parent, section["asset_id"], result, True, is_last, token)

def generate_guide_per_section(content, section, lang):
    prompt_esp = f"\nGenera directamente en español el contenido util, detallado, claro y extendida para una sección titulada '{section}'. Incluir código python necesario si lo ves conveniente. Toda la respuesta debe ser basado en la siguiente información del proyecto:\n\n{content}\n"
    prompt_eng = f"\nDirectly generate useful in english, detailed, clear and extended content for a section titled '{section}'. Include necessary python code if you see fit. All response should be based on the following project information:\n\n{content}\n"
    
    prompt = prompt_eng if lang == "English" else prompt_esp
    
    response = call_api_guia_with_retry(prompt, lang)
    
    return response
    
def update_guide(project_id, asset_parent, asset_id, content, success, isFinal, token):
    headers = {
        "Authorization": f"Bearer {token}",
        "content-type": "application/json",
    }
    
    payload = {
        "project_id": project_id,
        "asset_parent": asset_parent,
        "asset_id": asset_id,
        "content": content,
        "success": success,
        "isFinal": isFinal
    }
    
    api_codeia = os.getenv('API_CODEIA')
    response = requests.post(f'{api_codeia}project/guide-reference-completion/', headers=headers, json=payload)
    
    if response.status_code == 200:
        content_text = response.json()
        print(f'GuIA: Updated success project_id = {project_id}, asset_id = {asset_id}')
    else:
        print(f'GuIA: Updated failed project_id = {project_id}, asset_id = {asset_id}')
        print(response.text)
        
def restore_project(project_id, asset_parent, token):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    
    payload = {
        "project_id": project_id,
        "asset_parent": asset_parent
    }
    
    api_codeia = os.getenv('API_CODEIA')
    response = requests.post(f'{api_codeia}project/restore/', headers=headers, json=payload)
    
    if response.status_code == 200:
        print(f'GuIA: Restore project success project_id = {project_id}')
    else:
        print(f'GuIA: Restore project error project_id = {project_id}')
        print(response.text)
    
def update_project_guide_running(project_id, guide_running, token):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    
    payload = {
        "project_id": project_id,
        "guide_running": guide_running
    }
    
    api_codeia = os.getenv('API_CODEIA')
    response = requests.post(f'{api_codeia}project/running-guide/', headers=headers, json=payload)
    
    if response.status_code == 200:
        print(f'GuIA: Running project success project_id = {project_id}')
        return True
    else:
        print(f'GuIA: Running project error project_id = {project_id}')
        print(response.text)
        return False
        
def call_api_guia_with_retry(prompt, lang):
    
    system_prompt = os.getenv('PROMP_GUIA_ENG') if lang == "English" else os.getenv('PROMP_GUIA_ENG')

    headers = {
        "x-goog-api-key": os.getenv('API_GEMINI_KEY'),
        "Content-Type": "application/json",
    }

    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {
                        "text": system_prompt
                    }
                ]
            },
            {
                "role": "user",
                "parts": [
                    {
                        "text": prompt
                    }
                ]
            }
        ]
    }
    
    retries = 0
    print(f'GuIA: Calling API with retry: {retries}')
    while retries < MAX_RETRIES:
        response = requests.post('https://generativelanguage.googleapis.com/v1/models/gemini-pro:generateContent', headers=headers, json=payload)
        if response.status_code == 200:
            content = response.json()['candidates'][0]['content']['parts'][0]['text']
            print(f'GuIA: Generated success. {content}')
            return content
        else:
            if response.status_code == 429:  # Too Many Requests
                retries += 1
                if retries == MAX_RETRIES:
                    return None
                print(f"GuIA: Start retry after {RETRY_DELAY} seconds")
                time.sleep(RETRY_DELAY)
            else:
                print(f'GuIA: Generated failed with status code: {response.status_code}')
                print(f'GuIA: Generated failed: {response.text}')
                return None