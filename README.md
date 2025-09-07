# How to run 
Everything is in the docker-compose file.
- Start the docker-compose file: `docker-compose up --build`
- To see the output from a particular component, use: `docker-compose logs -f <component_name>`

# Decisions
1. The topic is created in the docker-compose file 
2. I downloaded the csv file and put it in the Cloud (Google)
   - I could have downloaded it into the docker container 
3. I wanted everything to run from the same docker-compose file
   - this gave me some timing issues 
4. Had to use com.opencsv in the Producer to read the csv file, as one column has commas in it
5. Docker file in two stages
    - build the maven project 
    - copy the jar to a smaller image and run it
    - 

# Questions 
* The last requirement changes everything 
   - it's no longer purely streaming, we need a concept of "done"
   - it's unusual also in that the aggregates are sent back to the producer 
* Have made assumptions as to what "aggregate" means 
