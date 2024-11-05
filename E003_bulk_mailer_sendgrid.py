"""
Script to bulk mail
Author: Albert Janse van Rensburg (NWU:21162395)
2024-07-03
"""

# Index
"""
ENVIRONMENT
IMPORT SETTINGS FILE
IMPORT MESSAGE FILE
OPEN THE MAIL SERVER
IMPORT MAIL FILE
END OF SCRIPT
"""


def bulk_mailer(send_mail: bool = False, start_number: int = 1):
    """
    Script to bulk mail
    :param send_mail: True if ok to send the mail. False to test the sending process.
    :param start_number: The record number to start sending in previous send was interrupted.
    :return: integer: The number of emails delivered
    """

    # Import Python libraries
    import csv
    import configparser
    import smtplib
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import (Mail, Email, From, To, Content, ReplyTo)

    # Import own definitions
    from _my_modules import funcconf
    from _my_modules import funcdatn
    from _my_modules import funcfile
    from _my_modules import funcsys

    """*************************************************************************
    ENVIRONMENT
    *************************************************************************"""

    # Variables
    s_script: str = "E003_bulk_mailer"
    s_function: str = "bulk_mailer"
    s_description: str = "Script to send bulk email messages"
    l_debug: bool = False
    return_message: str = ""
    send_counter: int = 0
    # file_path: str = r"C:/Users/Python/Nextcloud/Nwucadev/Bulkmail/20240709_nomination_scc_odl/"  # IA Server
    # file_path: str = r"D:/Nwu/Nextcloud/Nwucadev/Bulkmail/20240709_nomination_scc_odl/"  # Development
    # file_path: str = r"O:/Nwuaudit/Nwuaudit_albert/2024 Nwu/Audits/2.9.3.8 Verification audits/2.9.3.8.1115 Election src 2024 (AR)/WP00 Bulk mail/"
    file_path: str = r"D:/Albert/OneDrive/Nwuaudit/Nwuaudit_albert/2024 Nwu/Audits/2.9.3.8 Verification audits/2.9.3.8.1220 Election council convocation (AR)/WP00 Bulk mail/"
    setting_file: str = f"{file_path}setting.csv"
    mail_file: str = f"{file_path}mail_file.csv"
    message_file_text: str = f"{file_path}message_text.txt"
    message_file_html: str = f"{file_path}message_html.txt"
    result_detail_file: str = f"{file_path}result_detail.csv"
    result_log_file: str = f"{file_path}result_log.csv"

    # Log
    if l_debug:
        print(s_script.upper())
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: " + s_script.upper())
    funcfile.writelog("-" * len("script: "+s_script))
    funcfile.writelog('FUNCTION: ' + s_function.upper())
    funcfile.writelog("%t " + s_description)

    """*************************************************************************
    IMPORT SETTINGS FILE
    *************************************************************************"""
    if l_debug:
        print("IMPORT SETTINGS FILE")

    # Read the settings file and store in a tuple
    funcfile.writelog(f"%t IMPORT SETTINGS FILE: {setting_file}")
    setting_list = []
    try:

        with open(setting_file, newline='') as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader)
            for row in csvreader:
                if row[0] == "X":
                    continue
                setting_list.append(tuple(row))
            if l_debug:
                print(setting_list)

    except FileNotFoundError:

        return_message = "The settings file was not found."
        funcfile.writelog(f"%t {return_message}")
        if l_debug:
            print(f"{setting_file}")
        return return_message

    except IOError:

        return_message = "An error occurred trying to read the settings file."
        funcfile.writelog(f"%t {return_message}")
        if l_debug:
            print(f"{setting_file}")
        return return_message

    """*************************************************************************
    IMPORT MESSAGE FILES
    *************************************************************************"""
    if l_debug:
        print("IMPORT MESSAGE FILES")

    funcfile.writelog(f"%t IMPORT TEXT MESSAGE FILE: {message_file_text}")
    try:

        # Read the contents / body of the email message from a file
        # Open the file in read mode

        # Opening a file with explicit UTF-8 encoding
        """
        with open("yourfile.txt", "r", encoding="utf-8", errors="replace") as file:
            contents = file.read()
            print(contents)
        except UnicodeDecodeError as e:
        """

        with open(message_file_text, "r", encoding="utf-8", errors="replace") as file:
            # Read the entire content of the file
            message_content_text = file.read()

    except FileNotFoundError:

        return_message = "The text message file was not found."
        funcfile.writelog(f"%t {return_message}")
        if l_debug:
            print(f"{message_file_text}")
        return return_message

    except IOError:

        return_message = "An error occurred trying to read the text message file."
        funcfile.writelog(f"%t {return_message}")
        if l_debug:
            print(f"{message_file_text}")
        return return_message

    funcfile.writelog(f"%t IMPORT HTML MESSAGE FILE: {message_file_html}")
    try:

        # Read the contents / body of the email message from a file
        # Open the file in read mode
        with open(message_file_html, "r", encoding="utf-8", errors="replace") as file:
            # Read the entire content of the file
            message_content_html = file.read()

    except FileNotFoundError:

        return_message = "The html message file was not found."
        funcfile.writelog(f"%t {return_message}")
        if l_debug:
            print(f"{message_file_html}")
        return return_message

    except IOError:

        return_message = "An error occurred trying to read the html message file."
        funcfile.writelog(f"%t {return_message}")
        if l_debug:
            print(f"{message_file_html}")
        return return_message

    """*************************************************************************
    OPEN THE MAIL SERVER
    *************************************************************************"""
    if l_debug:
        print("OPEN THE MAIL SERVER")

    # Read from the configuration file
    config = configparser.ConfigParser()
    config.read('.config.ini')

    # Open the mail server
    server_connect: bool = False
    server_host = config.get('SENDGRID', 'server')
    funcfile.writelog(f"%t Open the {server_host} mail server.")

    try:

        # Server
        sendgrid = SendGridAPIClient(config.get('SENDGRID', 'api_key'))
        server_connect = True

    except Exception as e:

        message = e
        return_message = f"Mail server error: {message}"
        funcfile.writelog(f"%t MAIL SERVER ERROR: {message}")
        return return_message

    """*************************************************************************
    IMPORT MAIL FILE
    *************************************************************************"""
    if l_debug:
        print("IMPORT MAIL FILE")

    # Read the mail file
    if server_connect:

        funcfile.writelog(f"%t IMPORT MAIL FILE: {mail_file}")
        try:

            with open(mail_file, newline='') as csvfile:
                
                csvreader = csv.reader(csvfile)
                next(csvreader)
                result_log = []
                result_detail = []
                record_counter: int = 1
                for row in csvreader:

                    # Skip a number of records if needed
                    if record_counter < start_number:

                        # Update the result log
                        result_log.append((record_counter, row[2], "", "Skipped"))
                        result_detail.append((record_counter, server_host, "", "", "", "", "", "", "", "", "", "", "Skipped"))
                        # Increase the record number
                        record_counter = record_counter + 1
                        continue

                    else:

                        # Read the row into variables
                        send_time = funcdatn.get_now_file()
                        recipient_mail = row[0]
                        recipient_mail_alternate = row[8]
                        recipient_name = row[1]
                        recipient_username = row[2]
                        recipient_password = row[3]
                        recipient_setting = row[6]
                        if l_debug:
                            print(f"Recipient mail: {recipient_mail}")
                            print(f"Recipient mail alternate: {recipient_mail_alternate}")
                            print(f"Recipient name: {recipient_name}")
                            print(f"Recipient username: {recipient_username}")
                            print(f"Recipient password: {recipient_password}")
                            print(f"Recipient setting: {recipient_setting}")

                        # Read settings into variables based on the recipient setting
                        for setting in setting_list:
                            if len(setting) > 1 and setting[1] == recipient_setting:
                                from_address = setting[2]
                                from_name = setting[3]
                                reply_to_address = setting[4]
                                reply_to_name = setting[5]
                                recipient_subject = setting[6]
                                recipient_user1 = setting[7]
                                if l_debug:
                                    print(f"Server from mail: {from_address}")
                                    print(f"Recipient subject: {recipient_subject}")
                                    print(f"Recipient user1: {recipient_user1}")
                                break

                        # Replace the variables in the message content with the correct variables
                        updated_message_content_text = message_content_text.replace("%name%", recipient_name)
                        updated_message_content_text = updated_message_content_text.replace("%nwunumber%", recipient_username)
                        updated_message_content_text = updated_message_content_text.replace("%password%", recipient_password)
                        updated_message_content_text = updated_message_content_text.replace("%user1%", recipient_user1)
                        text_body = updated_message_content_text

                        # Replace the variables in the message content with the correct variables
                        updated_message_content_html = message_content_html.replace("%name%", recipient_name)
                        updated_message_content_html = updated_message_content_html.replace("%nwunumber%", recipient_username)
                        updated_message_content_html = updated_message_content_html.replace("%password%", recipient_password)
                        updated_message_content_html = updated_message_content_html.replace("%user1%", recipient_user1)
                        html_body = updated_message_content_html

                        message_send = Mail(
                            from_email=From(from_address, from_name),
                            to_emails=To(recipient_mail, recipient_name),
                            subject=recipient_subject,
                            plain_text_content=text_body,
                            html_content=html_body
                        )

                        # Add the reply_to field
                        message_send.reply_to = ReplyTo(reply_to_address, reply_to_name)

                        # Send the message via SMTP server.
                        if send_mail:

                            try:

                                response = sendgrid.send(message_send)
                                if response.status_code == 202:
                                    message = "Accepted"
                                elif response.status_code == 200:
                                    message = "Delivered"
                                elif response.status_code == 201:
                                    message = "Created"
                                elif response.status_code == 204:
                                    message = "No content"
                                else:
                                    message = "Requested"
                                display_message = f"{record_counter} {recipient_username} {message}"
                                print(display_message)
                                result_log.append((record_counter, recipient_username, send_time, message))
                                result_detail.append((record_counter, server_host, from_address, recipient_mail,
                                                      recipient_mail_alternate, recipient_username, recipient_password,
                                                      recipient_name, recipient_subject, recipient_setting,
                                                      recipient_user1, send_time,
                                                      message))
                                if l_debug:
                                    print("Response status code:")
                                    print(response.status_code)
                                    print("Response body:")
                                    print(response.body)
                                    print("Response headers:")
                                    print(response.headers)

                                send_counter = send_counter + 1
                                return_message = f"{send_counter} emails were requested."

                            except Exception as e:

                                message = e
                                result_log.append((record_counter, recipient_username, send_time, f"Error: {message}"))
                                result_detail.append((record_counter, server_host, from_address, recipient_mail,
                                                      recipient_mail_alternate, recipient_username, recipient_password,
                                                      recipient_name, recipient_subject, recipient_setting,
                                                      recipient_user1, send_time,
                                                      f"Error: {message}"))
                                display_message = f"{record_counter} {recipient_username} {message}"
                                print(display_message)

                        else:

                            result_log.append((record_counter, recipient_username, send_time, f"Tested"))
                            result_detail.append((record_counter, server_host, from_address, recipient_mail,
                                                  recipient_mail_alternate, recipient_username, recipient_password,
                                                  recipient_name, recipient_subject, recipient_setting, recipient_user1,
                                                  send_time, "Tested"))
                            display_message = f"{record_counter} {recipient_username} Tested"
                            print(display_message)

                    # Increase the record number
                    record_counter = record_counter + 1

            with open(result_log_file, 'w', newline='') as file:

                writer = csv.writer(file)
                # Optionally, write a header
                writer.writerow(['RECORD#', 'USERNAME', 'TIMESTAMP', 'RESULT'])
                # Write the data
                writer.writerows(result_log)

            with open(result_detail_file, 'w', newline='') as file:

                writer = csv.writer(file)
                # Optionally, write a header
                writer.writerow(['RECORD#', 'MAILSERVER', 'MAILFROM', 'TO', 'TO_ALTERNATE', 'USERNAME', 'PASSWORD', 'NAME', 'MAILSUBJECT', 'PARAMETER', 'SETTING_USER1', 'TIMESTAMP', 'RESULT'])
                # Write the data
                writer.writerows(result_detail)

        except FileNotFoundError:

            return_message = "The mail file was not found."
            funcfile.writelog(f"%t {return_message}")
            if l_debug:
                print(f"{mail_file}")
            return return_message

        except IOError:

            return_message = "An error occurred trying to read the mail file."
            funcfile.writelog(f"%t {return_message}")
            if l_debug:
                print(f"{mail_file}")
            return return_message

    """*****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    funcfile.writelog("END OF SCRIPT")
    if l_debug:
        print("END OF SCRIPT")

    return return_message


if __name__ == '__main__':

    from _my_modules import funcconf
    from _my_modules import funcsys

    try:

        return_message = bulk_mailer(True, 1)
        print(f"{return_message}")

    except Exception as e:

        funcsys.ErrMessage(e)
