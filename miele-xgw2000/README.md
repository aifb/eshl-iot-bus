Create the all-in-one jar package by running the following command at command prompt:

    mvn package

Then run the module with the following command:

    java -jar target/miele-xgw2000-adapter-0.0.1-SNAPSHOT.jar

For development, create Eclipse project files with the following command, then import this directory as "existing project":

    mvn eclipse:eclipse
