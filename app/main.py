from datetime import date, timedelta
from team_utilities import match_extraction, except_messgs
from ref_utilities import refreehist_extraction, refexcept_messgs
from var import testleagues_list, leagues_list
from config import settings
import smtplib, os
from email.message import EmailMessage


def main():
    '''Main function the runs the entire app workflow, from extraction of the individual team's
    match history to the referee's history and loads the data into the database.
    
    It also records the error log and sends it to the dev's/client's email'''
    today = date.today()
    tomorrow = date.today() + timedelta(days=1)
    print(today, tomorrow)

    if (today.day % 2) == 0:
        match_extraction(leagues_list, today, tomorrow)
        refreehist_extraction(leagues_list, today, tomorrow)

        #Concatenating error logs to send to email.
        email = f"Error Logs for {today} and {tomorrow} Extraction.\n\n"
        email = email + f"Teams' Match History\n"
        for item in list(except_messgs.keys()):
            if item == list(except_messgs.keys())[-1]:
                email = email + f"{item}: {except_messgs[item]}\n\n"
            else:
                email = email + f"{item}: {except_messgs[item]}\n"
        email = email + f"Referee's History\n"
        for item in list(refexcept_messgs.keys()):
            if item == list(refexcept_messgs.keys())[-1]:
                email = email + f"{item}: {refexcept_messgs[item]}\n\n"
            else:
                email = email + f"{item}: {refexcept_messgs[item]}\n"
        
        #Sends error message to Email for recording or review
        msg = EmailMessage()
        msg['Subject'] = f"Error Logs for {today} and {tomorrow} Extraction."
        msg['From'] = settings.email_address
        msg['To'] = "michaeligbomezie@gmail.com"
        msg.set_content(email)

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(settings.email_address, settings.email_password)
            smtp.send_message(msg)


if __name__ == '__main__':
    dyno_type = os.environ.get("DYNO")
    print(dyno_type)
    if ('run' in dyno_type) | ('scheduler' in dyno_type):
        main()