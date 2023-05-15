# newday-movies-app

This is a pyspark based app. We can run it on EMR and EMR-Serverless

### What it does?
* Read movies.dat and Ratings.dat files from S3.
* Perform transformations like getting 3 new columns for movies data.
* Getting each user's tope 3 movies based on the rating.
* Store the ouput on S3.

### Deploy to Amazon EMR :
* Deploy the repo to directory on S3.
* Launch a EMR cluster by providing the bootstrap file in bootstap-action method so our dependencies will get install on cluster.
* ssh into one of the nodes in the cluster.
* Run spark-submit
```
spark-submit --master yarn --deploy-mode client --py-files=s3://<bucket_name>/newday-movies-app/conf/config.py application.py
```

### Deploy to Amazon EMR-Serveless:
* Deploy the repo to directory on S3.
* Create and Launch Studio in EMR-serverless console.
* Create Application in EMR-serverless.
* Run below command with AWS CLI

```
aws emr-serverless start-job-run \
    --application-id application-id \
    --execution-role-arn job-role-arn \
    --name job-run-name \
    --job-driver '{
        "sparkSubmit": {
          "entryPoint": "s3://<bucket_name>/newday-movies-app/application.py",
          "entryPointArguments": [],
          "sparkSubmitParameters": "--conf spark.executor.cores=1 --conf spark.executor.memory=4g --conf spark.driver.cores=1 --conf spark.driver.memory=4g --conf spark.executor.instances=1 --py-files=s3://<bucket_name>/newday-movies-app/conf/config.py"
        }
    }'
```


### Enhancement scope:
* Exception Handling
* Can have more option in terms of writing output in different format.
