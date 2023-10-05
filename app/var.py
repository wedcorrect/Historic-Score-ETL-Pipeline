#Dictionary containing the urls of each league for the different sites extracted from.

leagues_list = {
    'English Premier League':['https://www.flashscore.nl/voetbal/engeland/premier-league/schema/', 'https://www.worldfootball.net/all_matches/eng-premier-league-2023-2024/', 'https://www.sofascore.com/tournament/football/england/premier-league/17#52186'],
    'Championship':['https://www.flashscore.nl/voetbal/engeland/championship/schema/','https://www.worldfootball.net/all_matches/eng-championship-2023-2024/', 'https://www.sofascore.com/tournament/football/england/championship/18#52367'],
    'EFL Championship':['https://www.flashscore.nl/voetbal/engeland/efl-cup/schema/', '', 'https://www.sofascore.com/tournament/football/england/efl-cup/21#52393'],
    'EFL Trophy':['https://www.flashscore.nl/voetbal/engeland/efl-trophy/schema/', 'https://www.worldfootball.net/all_matches/eng-efl-trophy-2023-2024/', ''],
    'La Liga':['https://www.flashscore.nl/voetbal/spanje/laliga/schema/','https://www.worldfootball.net/all_matches/esp-primera-division-2023-2024/', 'https://www.sofascore.com/tournament/football/spain/laliga/8#52376'],
    'La Liga2':['https://www.flashscore.nl/voetbal/spanje/laliga2/schema/', 'https://www.worldfootball.net/all_matches/esp-segunda-division-2023-2024/', 'https://www.sofascore.com/tournament/football/spain/laliga-2/54#52563'],
    'Bundesliga':['https://www.flashscore.nl/voetbal/duitsland/bundesliga/schema/', 'https://www.worldfootball.net/all_matches/bundesliga-2023-2024/', 'https://www.sofascore.com/tournament/football/germany/bundesliga/35#52608'],
    'Serie A':['https://www.flashscore.nl/voetbal/italie/serie-a/schema/', 'https://www.worldfootball.net/all_matches/ita-serie-a-2023-2024/', 'https://www.sofascore.com/tournament/football/italy/serie-a/23#52760'],
    'Serie B':['https://www.flashscore.nl/voetbal/italie/serie-b/schema/', 'https://www.worldfootball.net/all_matches/ita-serie-b-2023-2024/', 'https://www.sofascore.com/tournament/football/italy/serie-b/53#52947'],
    'Ligue 1':['https://www.flashscore.nl/voetbal/frankrijk/ligue-1/schema/', 'https://www.worldfootball.net/all_matches/fra-ligue-1-2023-2024/', 'https://www.sofascore.com/tournament/football/france/ligue-1/34#52571'],
    'Ligue 2':['https://www.flashscore.nl/voetbal/frankrijk/ligue-2/schema/', 'https://www.worldfootball.net/all_matches/fra-ligue-2-2023-2024/', 'https://www.sofascore.com/tournament/football/france/ligue-2/182#52572'],
    'Brasileirão Serie A':['https://www.flashscore.nl/voetbal/brazilie/braziliaanse-competitie/schema/', 'https://www.worldfootball.net/all_matches/bra-serie-a-2023/', 'https://www.sofascore.com/tournament/football/brazil/brasileiro-serie-a/325#48982'],
    'Primera División':['https://www.flashscore.nl/voetbal/argentinie/primera-d/schema/', '', ''],
    'Major League Soccer':['https://www.flashscore.nl/voetbal/usa/mls/schema/', 'https://www.worldfootball.net/all_matches/usa-major-league-soccer-2023/', 'https://www.sofascore.com/tournament/football/usa/mls/242#47955'],
    'Eredivisie':['https://www.flashscore.nl/eredivisie/schema/', 'https://www.worldfootball.net/all_matches/ned-eredivisie-2023-2024/', 'https://www.sofascore.com/tournament/football/netherlands/eredivisie/37#52554'],
    'Primeira Liga':['https://www.flashscore.nl/voetbal/portugal/liga-portugal/schema/', 'https://www.worldfootball.net/all_matches/por-primeira-liga-2023-2024/', 'https://www.sofascore.com/tournament/football/portugal/liga-portugal/238#52769'],
    'J1 League':['https://www.flashscore.nl/voetbal/japan/j1-league/schema/', 'https://www.worldfootball.net/all_matches/jpn-j1-league-2023/', 'https://www.sofascore.com/tournament/football/japan/j1-league/196#48055'],
    'Scottish Premiership':['https://www.flashscore.nl/voetbal/schotland/premiership/schema/', 'https://www.worldfootball.net/all_matches/sco-premiership-2023-2024/', 'https://www.sofascore.com/tournament/football/scotland/premiership/36#52588'],
    'Superliga':['https://www.flashscore.nl/voetbal/denemarken/superliga/schema/', 'https://www.worldfootball.net/all_matches/den-superligaen-2023-2024/', 'https://www.sofascore.com/tournament/football/denmark/superliga/39#52172'],
    'Süper Lig':['https://www.flashscore.nl/voetbal/turkije/super-lig/schema/', 'https://www.worldfootball.net/all_matches/tur-sueperlig-2023-2024/', 'https://www.sofascore.com/tournament/football/turkey/trendyol-super-lig/52#53190'],
    'Allsvenskan':['https://www.flashscore.nl/voetbal/zweden/allsvenskan/schema/', 'https://www.worldfootball.net/all_matches/swe-allsvenskan-2023/', 'https://www.sofascore.com/tournament/football/sweden/allsvenskan/40#47730'],
    'Saudi Professional League':['https://www.flashscore.nl/voetbal/saoedi-arabie/premier-league/schema/', 'https://www.worldfootball.net/all_matches/ksa-saudi-pro-league-2023-2024/', 'https://www.sofascore.com/tournament/football/saudi-arabia/saudi-professional-league/955#53241'],
    'Jupiler Pro League':['https://www.flashscore.nl/voetbal/belgie/jupiler-pro-league/schema/', 'https://www.worldfootball.net/all_matches/bel-eerste-klasse-a-2023-2024/', 'https://www.sofascore.com/tournament/football/belgium/pro-league/38#52383'],
    'UEFA Champions League':['https://www.flashscore.nl/voetbal/europa/champions-league/schema/', 'https://www.worldfootball.net/all_matches/champions-league-2023-2024/', 'https://www.sofascore.com/tournament/football/europe/uefa-champions-league/7#52162'],
    'UEFA Europa League':['https://www.flashscore.nl/voetbal/europa/europa-league/schema/', 'https://www.worldfootball.net/all_matches/europa-league-2023-2024/', 'https://www.sofascore.com/tournament/football/europe/uefa-europa-league/679#53654'],
    'UEFA Europa Conference League':['https://www.flashscore.nl/voetbal/europa/europa-conference-league/schema/', 'https://www.worldfootball.net/all_matches/europa-conference-league-2023-2024/', 'https://www.sofascore.com/tournament/football/europe/uefa-europa-conference-league/17015#52327'],
}


testleagues_list = {
    #'English Premier League':['https://www.flashscore.nl/voetbal/engeland/premier-league/schema/', 'https://www.worldfootball.net/all_matches/eng-premier-league-2023-2024/', 'https://www.sofascore.com/tournament/football/england/premier-league/17#52186'],
    #'Ligue 1':['https://www.flashscore.nl/voetbal/frankrijk/ligue-1/schema/', 'https://www.worldfootball.net/all_matches/fra-ligue-1-2023-2024/', 'https://www.sofascore.com/tournament/football/france/ligue-1/34#52571'],
    'La Liga':['https://www.flashscore.nl/voetbal/spanje/laliga/schema/','https://www.worldfootball.net/all_matches/esp-primera-division-2023-2024/', 'https://www.sofascore.com/tournament/football/spain/laliga/8#52376'],
    #'La Liga2':['https://www.flashscore.nl/voetbal/spanje/laliga2/schema/', 'https://www.worldfootball.net/all_matches/esp-segunda-division-2023-2024/', 'https://www.sofascore.com/tournament/football/spain/laliga-2/54#52563'],
    'Serie B':['https://www.flashscore.nl/voetbal/italie/serie-b/schema/', 'https://www.worldfootball.net/all_matches/ita-serie-b-2023-2024/', 'https://www.sofascore.com/tournament/football/italy/serie-b/53#52947'],
    #'Major League Soccer':['https://www.flashscore.nl/voetbal/usa/mls/schema/', 'https://www.worldfootball.net/all_matches/usa-major-league-soccer-2023/', 'https://www.sofascore.com/tournament/football/usa/mls/242#47955'],
    #'Bundesliga':['https://www.flashscore.nl/voetbal/duitsland/bundesliga/schema/', 'https://www.worldfootball.net/all_matches/bundesliga-2023-2024/', 'https://www.sofascore.com/tournament/football/germany/bundesliga/35#52608'],
}


exe_path = r'C:/Users/hp/Documents/Our Documents/Personal Development/Projects/Client Projects/Dexter Hadeveld (Upwork)/ETL-Pipeline-Sport-Insight/app/chromedriver-win64/chromedriver.exe'