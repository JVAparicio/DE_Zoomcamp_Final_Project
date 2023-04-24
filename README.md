## This project aims to answer two questions related to board games' rankings on BoardGameGeek:

* Which games spent the most time in the top 10 rankings?
* Which year had the better average for games that reached the top 100?

The analysis is based on the daily rankings of board games collected from the BoardGameGeek website and stored in CSV files in this GitLab repository.

## Data Collection
### To collect the data, we wrote a web scraper using Python and the Beautiful Soup library to scrape the daily rankings of board games from BoardGameGeek. The scraper collected the rankings for each day since January 1st, 2010, up to the most recent day at the time of data collection.

### The data was stored in CSV files, one file for each year. Each row in the CSV file represents a board game and includes the following information:

* Rank: the rank of the board game on the specific day
* Name: the name of the board game
* Year: the year the board game was released
* Geek Rating: a score that reflects the overall popularity and rating of the board game on BoardGameGeek

## Analysis
The analysis is performed using Python and the Pandas library. 

To answer the first question, we calculated the total number of days each board game spent in the top 10 rankings and identified the top 10 games with the highest number of days spent in the top 10.
To answer the second question, we calculated the average Geek Rating for each year for all board games that reached the top 100 rankings. We then identified the year with the highest average Geek Rating.

The results of the analysis are presented in Jupyter notebooks in this repository.

Files in the Repository
* data/: a directory containing the CSV files with the daily rankings for each year
* src/: a directory containing the Python code for the web scraper and the analysis
* results/: a directory containing the Jupyter notebooks with the analysis results
* README.md: this file, providing an overview of the project
* Reproducing the Analysis

## To reproduce the analysis, follow these steps:

    1. Clone this repository to your local machine.
    2. Install the required Python libraries listed in src/requirements.txt.
    3. Run the web scraper in src/scraper.py to collect the latest data from BoardGameGeek. The data will be stored in CSV files in the data/ directory.
    4. Run the analysis in the Jupyter notebooks in the results/ directory.
    
Note that data collection can take a long time, depending on how far back you want to go. Also, BoardGameGeek may update their website layout, which could break the scraper.