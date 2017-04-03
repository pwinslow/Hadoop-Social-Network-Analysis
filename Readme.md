# Breadth First Search of the Marvel Superhero Network   

Marvel comics has been making great super-hero movies for years now. However, some people may not realize that the story lines and characters in all of these movies have already appeared in comic books, some of which were published decades ago. Because these characters have been around so long, many of them have appeared in comic books with each other. The Marvel comics social network dataset, included in this repository, gives all the connections between each character based on appearances.    

This project allows the user to determine the minimum degrees of separation betweeen any two Marvel superhero characters by performing a breadth-first-search (BFS) on the Marvel social network.   

### Prerequisites   

Python 2.7    

This code makes use of map-reduce to perform the BFS analysis. However, I've set the code up to run without a cluster by simply using the MRJob python module with a local runner. The MRJob python module can be installed with pip as   

```
pip install mrjob
```   

### Example   

Once all files have been put into a single folder on your local machine, simply find your two favorite superheros in the Marvel-Names.txt file. Let's say you're an extremely avid reader and your favorite superheros are BLACKBODY and ARVAK. In the Marvel-Names.txt file, we find that their id's are 554 and 234, respectively. To determine their minimum degrees of separation, type   

```
python BFS_Runner.py --input_id=554 --target_id=234
```   

It should then return    

```
BLACKBODY is 3 degrees of separation away from ARVAK
```

## Acknowledgements   

This project grew out of the "Taming Big Data with MapReduce and Hadoop" MOOC on Udemy by Frank Kane
