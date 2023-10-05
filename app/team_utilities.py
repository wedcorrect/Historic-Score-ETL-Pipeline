from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent
from itertools import chain
import time, os, psycopg2, json, re
import pandas as pd
from sqlalchemy import create_engine
from var import exe_path
from config import settings

except_messgs = {}

def matches_scores(url):
    '''This function extracts the the historic match scores of the teams paired up for an upcoming match
    and transforms the data for further analaysis.'''
    options = Options()
    
    #Sets up a fake browser
    #services = Service(executable_path=exe_path)
    services = Service(executable_path=os.environ.get("CHROMEDRIVER_PATH"))
    ua = UserAgent()
    userAgent = ua.random
    options.add_argument(f'user-agent={userAgent}')
    options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
    options.add_argument('--blink-settings=imagesEnabled=false')
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(service=services, options=options)
    

    driver.get(url)
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="onetrust-accept-btn-handler"]'))).click()
    time.sleep(3)
    
    #sleeps because the page has to load the data before scraping h2h__showMore showMore
    
    elements = driver.find_elements(By.CLASS_NAME, "showMore")
    for elem in elements:
        elem.click()
    contents = driver.find_elements(By.CLASS_NAME, "h2h")
    
    #gets the data by class name
    content = [con.get_attribute('innerText') for con in contents]
    
    #extracts the content of each H2H page and cleans it
    clean = [item.replace('\n', ',') for item in content]
    clean = [item.split(',') for item in clean]
    clean = list(chain(*clean))
    remove = ['Toon meer wedstrijden']
    clean  = [word for word in clean if word not in remove ]
    
    driver.quit()
    
    #Using the table sub-table heads to split the data between the last 10 matches for the home and away teams and their head2head
    start_indices = [i for i, item in enumerate(clean) if "LAATSTE WEDSTRIJDEN" in item or "HEAD-TO-HEAD" in item]
    end_indices = start_indices[1:] + [len(clean)]
    first_list = clean[start_indices[0]:end_indices[0]]
    second_list = clean[start_indices[1]:end_indices[1]]
    third_list = clean[start_indices[2]:end_indices[2]]

    #Removes the sub-table heads to only have the useful information for the analysis
    first_list = [word for word in first_list if 'LAATSTE WEDSTRIJDEN' not in word]
    second_list = [word for word in second_list if 'LAATSTE WEDSTRIJDEN' not in word]
    third_list = [word for word in third_list if 'HEAD-TO-HEAD' not in word]

    #Adds all the home historic match outcomes to a dictionary
    home_team_matches = {'date': [],'league': [],'home_club': [],'away_club': [],'home_club_goal': [],'away_club_goal': []}
    keys = list(home_team_matches.keys())
    count_1 = 0
    for item in first_list:
        if count_1 <= 5:
            home_team_matches[keys[count_1]].append(item)
            count_1 += 1
        else:
            #Account for instance there are some unexpected values within the data being extracted
            try:
                value = int(item)
                continue
            except:
                count_1 = 0
                continue
    home_team_matches = json.dumps(home_team_matches)

    #Adds all the away historic match outcomes to a dictionary
    away_team_matches = {'date': [],'league': [],'home_club': [],'away_club': [],'home_club_goal': [],'away_club_goal': []}
    keys = list(away_team_matches.keys())
    count_1 = 0
    for item in second_list:
        if count_1 <= 5:
            away_team_matches[keys[count_1]].append(item)
            count_1 += 1
        else:
            #Account for instance there are some unexpected values within the data being extracted
            try:
                value = int(item)
                continue
            except:
                count_1 = 0
                continue
    away_team_matches = json.dumps(away_team_matches)

    #Adds all the head2head historic match outcomes to a dictionary
    head2head_matches = {'date': [],'league': [],'home_club': [],'away_club': [],'home_club_goal': [],'away_club_goal': []}
    keys = list(head2head_matches.keys())
    count_1 = 0
    for item in third_list:
        if count_1 <= 5:
            head2head_matches[keys[count_1]].append(item)
            count_1 += 1
        else:
            #Account for instance there are some unexpected values within the data being extracted
            if len(item) > 3:
                head2head_matches['date'].append(item)
                count_1 = 1
            else:    
                continue
    head2head_matches = json.dumps(head2head_matches)

    #Account for instances where no matches have been played
    return home_team_matches, away_team_matches, head2head_matches


def activity_times(content):
    '''Takes in the match content and extracts the time (also calculated the overtime)'''
    #print(content)
    activity_time = [con.replace("\n","'") for con in content]
    activity_time = [con.split("'")[0] for con in activity_time]
    for ind in range(len(activity_time)):
        if '+' in activity_time[ind]:
            temp_vars = activity_time[ind].split('+')
            temp_var = int(temp_vars[0])+int(temp_vars[1])
            activity_time[ind] = str(temp_var)
    return(activity_time)


def activity_type(activities):
    '''Takes in the content of the match, cleans it and extract the type of activity'''
    #print(activities)
    activities_list = []
    delim = r'[<>]'
    diction = {'card':'red-yellowcard','red':'redcard','soccer':'goal','substitution':'substitution','var':'var','warning':'penalty(missed)','yellow':'yellowcard'}
    for activity in activities:
        temp_list = [i for i in re.split(delim, activity) if 'svg class' in i]
        
        chosen_activity = []
        for key in diction.keys():
            if key in temp_list[0]:
                #This specifically checks for red-yellow card, as it is difficult to distinguish from red or yellow card by just 'svg class'
                if key == 'card':
                    if ('red' not in temp_list[0]) & ('yellow' not in temp_list[0]):
                        chosen_activity.append(diction['card'])
                else:
                    chosen_activity.append(diction[key])
                    
        activities_list.append(chosen_activity[0])
    return(activities_list)


def matches_details(team, url):
    '''This function simply extracts the inner match details of the historic matches of the teams under analysis'''
    team = team
    url = url
    
    #Sets up a fake browser
    #services = Service(executable_path=exe_path)
    services = Service(executable_path=os.environ.get("CHROMEDRIVER_PATH"))
    options = Options()
    ua = UserAgent()
    userAgent = ua.random
    options.add_argument(f'user-agent={userAgent}')
    options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
    options.add_argument('--blink-settings=imagesEnabled=false')
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(service=services, options=options)

    #Loads up the url using the chromedriver and clicks the cookie prompt
    driver.get(url)
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="onetrust-accept-btn-handler"]'))).click()
    
    #Extracts the data in the table rows (containing links to previous match)
    elements = driver.find_elements(By.CLASS_NAME, "h2h__row")#'Klik voor wedstrijddetails!'
          
    list_of_details = {}
    
    #Loops through each match link to extract the url of each page (match details)
    for count in range(len(elements[:5])):
        try:
            #Sets up a fake browser
            #services = Service(executable_path='../chromedriver-win64/chromedriver.exe')
            options = Options()
            ua = UserAgent()
            userAgent = ua.random
            options.add_argument(f'user-agent={userAgent}')
            options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
            options.add_argument('--blink-settings=imagesEnabled=false')
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")

            driver = webdriver.Chrome(service=services,options=options)

            #Loads up the url using the chromedriver and clicks the cookie prompt
            driver.get(url)
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="onetrust-accept-btn-handler"]'))).click()
            time.sleep(1.5)

            #Extracts the data in the table rows (containing links to previous match)
            element = driver.find_elements(By.CLASS_NAME, "h2h__row")
            elem = element[count]
            elem.click()
            driver.switch_to.window(driver.window_handles[-1]) #Switches the driver to the new opened page
            
            #checks which role the team under consideration played as (home or away) in the match being checked
            match_date = driver.find_element(By.CLASS_NAME, 'duelParticipant__startTime')
            match_date = match_date.get_attribute('textContent')
            #print(match_date)

            tournament = driver.find_element(By.CLASS_NAME, 'tournamentHeader__country')
            tournament = tournament.get_attribute('textContent')
            #print(tournament)
            
            match_score = driver.find_element(By.CLASS_NAME, 'detailScore__wrapper')
            match_score = match_score.get_attribute('textContent')
            #print(match_score)

            try:
                home_team = driver.find_element(By.CLASS_NAME, 'duelParticipant__home')
                home_teamname = home_team.get_attribute('textContent')
            except:
                home_team = driver.find_element(By.CLASS_NAME, 'duelParticipant__home duelParticipant--winner')
                home_teamname = home_team.get_attribute('textContent')
            try:
                away_team = driver.find_element(By.CLASS_NAME, 'duelParticipant__away ')
                away_teamname = away_team.get_attribute('textContent')
            except:
                away_team = driver.find_element(By.CLASS_NAME, 'duelParticipant__away duelParticipant--winner')
                away_teamname = away_team.get_attribute('textContent')
            #print(home_teamname, away_teamname)
            
            #Based on which role, the class name is decided and used to extract details
            if team in home_teamname:
                class_name_1 = 'smv__participantRow.smv__homeParticipant'
                class_name_2 = 'smv__participantRow.smv__awayParticipant'
            elif team in away_teamname:
                class_name_1 = 'smv__participantRow.smv__awayParticipant'
                class_name_2 = 'smv__participantRow.smv__homeParticipant'   
            #print(class_name_1, class_name_2)
            
            #Extracts the contents fo the match
            contents_1 = driver.find_elements(By.CLASS_NAME, class_name_1)
            contents_2 = driver.find_elements(By.CLASS_NAME, class_name_2)
            #print(len(contents_1), len(contents_2))
            
            content_1 = [con.get_attribute('innerText') for con in contents_1]
            activities_1 = [con.get_attribute('innerHTML') for con in contents_1]
            #print(len(content_1), len(activities_1))
            
            content_2 = [con.get_attribute('innerText') for con in contents_2]
            activities_2 = [con.get_attribute('innerHTML') for con in contents_2]
            #print(len(content_2), len(activities_2))
            
            driver.close()

            #Extracts the activities time and type and zips the lists to be processed together
            activities_time_1 = activity_times(content_1)
            activities_list_1 = activity_type(activities_1)
            #print(len(activities_time_1), len(activities_list_1))
            combined_list_1 = list(zip(activities_time_1, activities_list_1))
            #print(combined_list_1)

            #Groups the match activities time by the type of activity (yellow card, substitution etc.)
            details_1 = {'tournament':[tournament],'date':[match_date],'teams':[home_teamname,away_teamname],
                       'match_score':[match_score],'goal':[],'penalty(missed)':[],'redcard':[],'red-yellowcard':[],'substitution':[],
                       'var':[],'yellowcard':[]}
            for entry in combined_list_1:
                details_1[entry[1]].append(entry[0])
            #print(details_1)

            #Extracts the activities time and type and zips the lists to be processed together
            activities_time_2 = activity_times(content_2)
            activities_list_2 = activity_type(activities_2)
            #print(len(activities_time_2), len(activities_list_2))
            combined_list_2 = list(zip(activities_time_2, activities_list_2))
            #print(combined_list_2)
            

            #Groups the match activities time by the type of activity (yellow card, substitution etc.)
            details_2 = {'tournament':[tournament],'date':[match_date],'teams':[home_teamname,away_teamname],
                       'match_score':[match_score],'goal':[],'penalty(missed)':[],'redcard':[],'red-yellowcard':[],'substitution':[],
                       'var':[],'yellowcard':[]}
            for entry in combined_list_2:
                details_2[entry[1]].append(entry[0])
            #print(details_2)

            #Appends to the list of details which will be changed to dictionary
            list_of_details[str(count)] = {}
            list_of_details[str(count)]['team'] = details_1
            list_of_details[str(count)]['opponent'] = details_2
            driver.switch_to.window(driver.window_handles[0])
            driver.close()
        except Exception as e:
            setup = f"{team}:{url} ({count}) (Inner-Match-Det)"
            except_messgs[f"({setup})"] = f"{type(e).__name__}: {str(e).split('Stacktrace:')[0]}" #Catches and Records Error
            try:
                driver.close()
                list_of_details[str(count)] = {}
            except:
                list_of_details[str(count)] = {}
                  
    driver.quit()
    list_of_details = json.dumps(list_of_details)

    return list_of_details 


def data_loader(dataset):
    '''Extracting the data from the dataframe to load into the database multiple rows at a time'''

    #PostgreSQL database connection parameters
    connection_params = {
        "host": settings.database_hostname,
        "port": settings.database_port,
        "database": settings.database_name,
        "user": settings.database_user,
        "password": settings.database_password
    }

    #Connect to PostgreSQL
    connection = psycopg2.connect(**connection_params)
    cursor = connection.cursor()

    #Create the table in the database
    create_query = '''CREATE TABLE IF NOT EXISTS historic_match (
        date VARCHAR,
        hometeam VARCHAR,
        awayteam VARCHAR,
        match_urls VARCHAR,
        home_urls VARCHAR,
        away_urls VARCHAR,
        league VARCHAR,
        home_team_matches JSONB,
        away_team_matches JSONB,
        head2head_matches JSONB,
        home_team_matchespattern JSONB,
        away_team_matchespattern JSONB
    );'''
    cursor.execute(create_query)
    connection.commit()

    # Create a SQLAlchemy engine
    engine = create_engine(f'postgresql+psycopg2://{settings.database_user}:{settings.database_password}@{settings.database_hostname}/{settings.database_name}')

    dataset.to_sql('historic_match', engine, if_exists='append', index=False)

    #Commit and close connection
    connection.commit()
    cursor.close()
    connection.close()


def match_extraction(leagues_list, today, tomorrow):
    leagues_dataset = {} #Created the empyt dictionary that will be used to concatenate all table from all leagues
    
    for key in list(leagues_list.keys()):
        league_counter = 0
        try:
            '''To make sure all links load, irrespective of poor network or site loading wrongly,
            add a while loop which checks if a variable has been changed. If site loads properly,
            change variable to exit while loop, but if webiste threw an except (Timeout) message,
            keep variable the same to maintain the while loop until data is gotten'''
            
            league_url = leagues_list[key][0]
            print(league_url)

            #Sets up a fake browser
            #services = Service(executable_path=exe_path)
            services = Service(executable_path=os.environ.get("CHROMEDRIVER_PATH"))
            options = Options()
            ua = UserAgent()
            userAgent = ua.random
            options.add_argument(f'user-agent={userAgent}')
            options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
            options.add_argument('--blink-settings=imagesEnabled=false')
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")

            driver = webdriver.Chrome(service=services, options=options)

            #Loads up the url using the chromedriver and clicks the cookie prompt
            driver.get(league_url) 
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="onetrust-accept-btn-handler"]'))).click()
            
            #Extracts the contents of the table of scheduled matches
            contents = driver.find_elements("xpath", '//*[@title ="Klik voor wedstrijddetails!"]')
            content = [con.get_attribute('innerText') for con in contents]
            id_match = [con.get_attribute('id') for con in contents]

            driver.quit() #Closes driver to begin the transformation

            #Transformation: sets up urls to the head2head, home and away team form for previous matches
            clean= [entry.replace('\n-\n-', '').replace('\n', ',').split(',') for entry in content]
            
            #This filters the scheduled matches of any cancelled matches
            zipped_lists = zip(clean, id_match)
            zipped_lists = [pair for pair in zipped_lists if len(pair[0]) == 3]
            clean, id_match = zip(*zipped_lists)
            
            #Prepares the links needed to extract the historic match scores and in-match details
            base_url = 'https://www.flashscore.nl/wedstrijd/'
            overall_urls = [f"{base_url}{string.replace('g_1_', '')}/#/h2h/overall" for string in id_match]
            home_urls = [f"{base_url}{string.replace('g_1_', '')}/#/h2h/home" for string in id_match]
            away_urls = [f"{base_url}{string.replace('g_1_', '')}/#/h2h/away" for string in id_match]
            df = pd.DataFrame(clean, columns=['date', 'hometeam', 'awayteam'])
            df['match_urls'] = overall_urls
            df['home_urls'] = home_urls
            df['away_urls'] = away_urls
            
            #Converting the date columns to datetime
            df['date'] = pd.to_datetime(df['date'] + '.2023', format='%d.%m. %H:%M.%Y')

            #filters the dataframe prepared using the current date
            today_df = df[(df['date'].dt.date == today) | (df['date'].dt.date == tomorrow)]
            today_df = today_df.copy(deep=True)
            curr_league = [key for i in range(len(today_df['match_urls']))]
            today_df['league'] = curr_league

            hometeam_form = []
            awayteam_form = []
            head2head = []
            home_details = []
            away_detials = []

            #for each match url extract the head2head, home team and away team games score for the last 10 recent games
            for i in range(len(list(today_df['match_urls']))):
                match_url = list(today_df['match_urls'])[i]
                print(match_url)
                setup_1 = f"{list(today_df['hometeam'])[i]}:{list(today_df['awayteam'])[i]} (Historic Score)"
                try:
                    home_team_matches, away_team_matches, head2head_matches = matches_scores(match_url)
                    hometeam_form.append(home_team_matches)
                    awayteam_form.append(away_team_matches)
                    head2head.append(head2head_matches)
                    print('done!')
                except Exception as e:
                    except_messgs[str(key)+f": {league_counter} ({setup_1})"] = f"{type(e).__name__}: {str(e).split('Stacktrace:')[0]}" #Catches and Records Error
                    league_counter += 1
                    empty_json = json.dumps({})
                    hometeam_form.append(empty_json)
                    awayteam_form.append(empty_json)
                    head2head.append(empty_json)
                    print('except!')
            
            #extract the match detail (yellow cars, goals, penalties and times etc) for last 10 games by home team
            for i in range(len(list(today_df['home_urls']))):
                home_url = list(today_df['home_urls'])[i]
                print(home_url)
                home_team = list(today_df['hometeam'])[i]
                setup = f"{list(today_df['hometeam'])[i]}:{list(today_df['awayteam'])[i]} (Home Inner-Det)"
                try:
                    home_team_dets = matches_details(home_team, home_url)
                    home_details.append(home_team_dets)
                    print('done!')
                except Exception as e:
                    except_messgs[str(key)+f": {league_counter} ({setup})"] = f"{type(e).__name__}: {str(e).split('Stacktrace:')[0]}" #Catches and Records Error
                    league_counter += 1
                    empty_json = json.dumps({})
                    home_details.append(empty_json)
                    print('except!')
            
            #extract the match detail (yellow cars, goals, penalties and times etc) for last 10 games by away team
            for i in range(len(list(today_df['away_urls']))):
                away_url = list(today_df['away_urls'])[i]
                print(away_url)
                away_team = list(today_df['awayteam'])[i]
                setup = f"{list(today_df['hometeam'])[i]}:{list(today_df['awayteam'])[i]} (Away Inner-Det)"
                try:
                    away_team_dets = matches_details(away_team, away_url)
                    away_detials.append(away_team_dets)
                    print('done!')
                except Exception as e:
                    except_messgs[str(key)+f": {league_counter} ({setup})"] = f"{type(e).__name__}: {str(e).split('Stacktrace:')[0]}" #Catches and Records Error
                    league_counter += 1
                    empty_json = json.dumps({})
                    away_detials.append(empty_json)
                    print('except!')   

            #Add all these extracted data to the dataframe of daily match of the current league being extracted
            today_df['home_team_matches'] = hometeam_form
            today_df['away_team_matches'] = awayteam_form
            today_df['head2head_matches'] = head2head
            today_df['home_team_matchespattern'] = home_details
            today_df['away_team_matchespattern'] = away_detials
            
            
            #Loads the extracted league to the database
            for i in range(2): #Tries twice to load data in case of any unforeseen connection issue
                try:
                    data_loader(today_df) #If try is successful, breaks the loop
                    print("All daily matches of {} have been loaded!".format(key))
                    break
                except Exception as e:
                    except_messgs[str(key)+f": {league_counter} (Database Loading)"] = f"{type(e).__name__}: {str(e).split('Stacktrace:')[0]}" #Catches and Records Error
                    league_counter += 1
                    print("All daily matches of {} couldn't be loaded!".format(key))
                    if i < 1: #If try isn't successful but it's the first time, then it tries again
                        continue
                    else: #If try isn't successful the second time, it adds the dataframe to the dictionary to try later.
                        leagues_dataset[key] = today_df
                    
        except Exception as e:
            except_messgs[str(key)+f": {league_counter}"] = f"{type(e).__name__}: {str(e).split('Stacktrace:')[0]}" #Catches and Records Error
            league_counter += 1
            try:
                driver.quit()
                print("Daily matches of {} couldn't be extracted!".format(key))
                continue
            except:
                print("Daily matches of {} couldn't be extracted!".format(key))
                continue
     
    #All the dataframe of the daily matches for all the leagues extracted are concatenated vertically
    list_of_keys = list(leagues_dataset.keys())
    if len(list_of_keys) > 0:
        for i in range(len(list_of_keys)):
            if i == 0:
                key = list_of_keys[i]
                final_dataset = leagues_dataset[key].copy(deep=True)
            else:
                key = list_of_keys[i]
                final_dataset = pd.concat([final_dataset, leagues_dataset[key]], axis=0)

        #Retries to load all the previous data that couldn't be loaded during extraction into the database
        for i in range(2): #Tries twice to load data in case of any unforeseen connection issue
            try:
                data_loader(final_dataset) #If try is successful, breaks the loop
                break
            except Exception as e:
                except_messgs[f"(Final Database Loading): {i}"] = f"{type(e).__name__}: {str(e).split('Stacktrace:')[0]}" #Catches and Records Error
                league_counter += 1
                continue #If try isn't successful but it's the first time, then it tries again
        