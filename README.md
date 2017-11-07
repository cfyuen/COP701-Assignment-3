# COP701-Assignment-3
Data mining using Hadoop

### Running Sequence
1. Solved Count - Indicate how many users have solved each problem
2. Median Solved Rating - For each problem, the median of rating of all solved users
    - Input: user.ratedList.json + data
    - Output: A text file which should be renamed to problem.rating.txt 
3. User By Genre Rating - For each user, output the top 10 problems solved rating in each genre
    - Input: problem.rating.txt + data
    - Output: A text file which should be renamed to user.genre.rating.txt, then copy to user_rating folder
4. Country By Genre Rating - For each country, output the top K% (default = 10%) of user rating in each genre
    - Input: user.ratedList.json + user_rating
    - Output: A text file which should be renamed to country.genre.rating.txt