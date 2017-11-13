# COP701-Assignment-3
Data mining using Hadoop

### Running Sequence
1. Solved Count - Indicate how many users have solved each problem
    - Input: data
    - Output: A text file which should be renamed to solved.count.txt
2. Median Solved Rating - For each problem, the median of rating of all solved users
    - Input: user.ratedList.json + data
    - Output: A text file which should be renamed to problem.rating.txt
3. Problem Genre - For each problem, the genre of the problem
    - Input: data
    - Output: A text file which should be renamed to problem.genre.txt 
4. User By Genre Rating - For each user, output the top 10 problems solved rating in each genre
    - Input: problem.rating.txt + data
    - Output: A text file which should be renamed to user.genre.rating.txt, then copy to user_rating folder
5. Country By Genre Rating - For each country, output the top K% (default = 10%) of user rating in each genre
    - Input: user.ratedList.json + user_rating
    - Output: A text file which should be renamed to country.genre.rating.txt
6. Next to Solve
    - Args: handle, genre
    - Input: user_rating/user.genre.rating.txt + solved_count/solved.count.txt + problem/problem.genre.txt + problem/problem.rating.txt + data/<handle>.json
    - Output: Up to 5 problems to solve next 
	