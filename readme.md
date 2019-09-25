# Amazon Athena Spring Boot POC

This is a simple Spring Boot POC to demonstrate how we can query an Amazon Athena table from 
a Spring Boot application, or any other Java code in general.

# How to run?

In the ```App.java``` class, make sure you change the values of ```ATHENA_DATABASE``` and ```ATHENA_OUTPUT_S3_FOLDER_PATH```
 constants. Also, in the ```AthenaClientFactory.java``` class, change the region accordingly.
 
 Once this is done, run the program and you should see the output.

`