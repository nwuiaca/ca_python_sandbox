"""
Script to query chatGPT
Author: Albert B Janse van Rensburg (NWU:21162395)
Created: 13 April 2023
"""

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcsys

# INDEX
"""
ENVIRONMENT
COMPLETION REQUEST
END OF SCRIPT
"""

# VARIABLES
s_script: str = "E001_ai_request"


def ai_completion(s_prompt: str = '', s_role: str = '', s_name: str = '', s_span: str = '', i_token: int = 1024):
    """
    Script to query chatGPT completion
    :param s_prompt: The query.
    :param s_name: The name of the requester / recipient
    :param i_token: Maximum tokens (max < 4096)
    :return: str: The chatbot completion message
    """

    # IMPORT PYTHON MODULES
    import openai
    import configparser
    from datetime import datetime

    # IMPORT OWN MODULES
    from _my_modules import funcfile
    from _my_modules import funcsms

    # DECLARE VARIABLES
    l_debug: bool = False

    """*************************************************************************
    ENVIRONMENT
    *************************************************************************"""
    if l_debug:
        print("ENVIRONMENT")

    # DECLARE VARIABLES
    # Read from the configuration file
    config = configparser.ConfigParser()
    config.read('.config.ini')

    s_function: str = 'ai_completion'
    s_description: str = "chatGPT completion request"
    # l_mess: bool = funcconf.l_mess_project
    l_mess: bool = True

    # Exit function if no prompt
    if s_prompt == '':
        return 'No prompt given!'
    if s_name == '':
        s_name = 'Unknown'

    # LOG
    if l_debug:
        print(s_script.upper())
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: " + s_script.upper())
    funcfile.writelog("-" * len("script: "+s_script))
    funcfile.writelog('FUNCTION: ' + s_function.upper())
    funcfile.writelog("%t " + s_description + " by " + s_name)

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", "<b>" + s_script.upper() + "</b>")

    """*************************************************************************
    COMPLETION REQUEST
    *************************************************************************"""
    if l_debug:
        print("COMPLETION REQUEST")

    # Initiate chatGPT
    openai.organization = config.get('API', 'token_chatgpt_organization')
    openai.api_key = config.get('API', 'token_chatgpt')

    # Build the prompt
    # Use span to decide if an auditor or else.
    if s_span == '0':
        s_order: str = """
        You are a virtual internal auditor at a university and will be a friendly coach and mentor to your fellow auditors.
        Your name is Caria, an acronym for Continuous Audit Robot from Internal Audit.
        The name of the auditor you are talking to is %NAME%.
        Our audit clients are employees of the university, from executive management to general workers. 
        You will reply with a funny remark if a fellow auditor ask something not related to audit work. 
        Our university is Nort-West University in South Africa, and we have three campuses.
        The larger one in Potchefstroom where the auditors are based, and a campus in Mahikeng, and another campus in Vanderbijlpark. 
        The financial and general ledger system used is called KFS. 
        The student administration system is called VSS.
        The employee administration system is called P&C System.
        The audit areas we focus on include follow-up, assurance, adhoc, compliance, verification, year-end, significant risk, continuous and special investigations.
        Our audits should be conducted according to the International Standards for Professional Practice of Internal Auditing.
        You are also a very experienced python programmer and will help to write effective python scrips and functions for our continuous audit system.
        """
    elif s_span == '1':
        s_order: str = """
        You are a virtual employee at a university in the Corporate and Information Governance Services department.
        You will be a friendly assistant, coach and mentor to your fellow employees.
        Your name is Caria.
        The name of your fellow employee you are talking to is %NAME%.
        You will reply with a funny remark if a fellow employee ask something not related to Corporate and Information Governance Services. 
        Our university is Nort-West University in South Africa, and we have three campuses.
        The larger one in Potchefstroom where we are based, and a campus in Mahikeng, and another campus in Vanderbijlpark.
        Against the background of the Gartner definition of information governance being “the specification of 
        decision rights and an accountability framework to ensure appropriate behaviour in the valuation, 
        creation, storage, use, archiving and deletion of information”, the university views information governance
        as an overarching framework for oversight of information and the processes by which it is generated, 
        processed, and curated at the university.
        """
    else:
        s_order: str = """
        You are a friendly AI assistant and mentor who assist with any general knowledge inquiries.
        You will help with any historical event, scientific concept, geographical fact, or even a philosophical question.
        You are here to help someone expand their knowledge and satisfy their curiosity.
        Your name is Sparkle.
        The name of the person you are talking to is %NAME%.
        You may be funny at times.
        """
    s_order = s_order.replace("%NAME%", s_name)

    # Feed the prompt
    response = openai.ChatCompletion.create(
        model="gpt-4o-2024-08-06",
        # model="gpt-4o-2024-05-13",
        messages=[
            {"role": "system", "content": s_order},
            {"role": "user", "content": s_prompt}
        ]
    )
    # OpenAI models
    # gpt-4 Started with this one
    # gpt-4-1106-preview Released 20231107
    # gpt-4o-2024-05-13 Released 20240514

    # Print the response
    if l_debug:
        print(response.choices[0].message)
        print(response)

    # Build response text
    response_text: str = response['choices'][0]['message']['content']
    # response_text = response_text.replace('\n', '')

    # Print some response parameters
    if l_debug:
        print('       Object: ' + response['object'])
        print('        Model: ' + response['model'])
        print('         Role: ' + response['choices'][0]['message']['role'])
        print('       Prompt: ' + s_prompt)
        print('Finish reason: ' + response["choices"][0]["finish_reason"])
        print('        Index: ' + str(response["choices"][0]["index"]))
        print('      Created: ' + str(response["created"]))
        print('  Response_id: ' + response["id"])

    # Log
    funcfile.writelog("MODEL: " + response["model"])
    funcfile.writelog("OBJECT: " + response["object"])
    funcfile.writelog("PROMPT: (" + str(response["usage"]["prompt_tokens"]) + ") " + s_prompt)
    funcfile.writelog("RESPONSE: (" + str(response["usage"]["completion_tokens"]) + ") " + response_text)

    # MESSAGE
    s_message = f'{s_name} prompted ' + response['model'] + f' with {s_prompt}'
    if l_mess:
        funcsms.send_telegram("", "administrator", s_message)

    # Build the return message
    s_return_message: str = response_text

    """*****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    funcfile.writelog("END OF SCRIPT")
    if l_debug:
        print("END OF SCRIPT")

    # CLOSE THE LOG WRITER
    funcfile.writelog("-" * len("completed: "+s_script))
    funcfile.writelog("COMPLETED: " + s_script.upper())

    return s_return_message[0:4096]


if __name__ == '__main__':
    try:
        s_return = ai_completion('What is the meaning of life?', 'Albert', 2056)
        print("RETURN: " + s_return)
        print("LENGTH: " + str(len(s_return)))
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, s_script, s_script)
