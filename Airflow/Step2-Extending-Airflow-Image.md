##### Extending Docker Image

[Production Deployment — Airflow Documentation (apache.org)](https://airflow.apache.org/docs/apache-airflow/stable/production-deployment.html)

1. **delete** the  first `DAG`

   - what happens after deleting a DAG?
   - check airflow meta database  
   - you must schedule some tasks to clean up the metadata database,  but 
   - there are so many operator to Automate your tasks!

2. Change the dag mount point in `docker-compose-apache.yml` from `./dags` to `./dags-v2`

3. Restart the cluster and see the errors

4. Create a custom `Dockerfile` that install Linux libraries and python packages 

   ```dockerfile
   FROM apache/airflow:2.0.1
   USER root
   RUN apt-get update \
     && apt-get install -y --no-install-recommends \
            build-essential tzdata \
     && apt-get autoremove -yqq --purge \
     && apt-get clean \
     && rm -rf /var/lib/apt/lists/*
   RUN pip install --no-cache-dir --user jdatetime \
         azure-storage-blob \
         apache-airflow-providers-apache-hdfs \
         apache-airflow-providers-microsoft-azure \
         hdfs
   ENV TZ=Asia/Tehran
   RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
   USER airflow
   
   ```

   **we set the time zone to `Asia/Tehran`**

5. Build the image  : 

```bash

 $ docker build -t nikamooz/airflow:2.0.1 .

```



3. check the image : `docker image ls` 

4.  Set image name in `.env` file. (*rename .env_sample to .env*)

   ```bash
   AIRFLOW_IMAGE_NAME=nikamooz/airflow:2.0.1
   ```

   

5. change the dag mount point in `docker-compose-apache.yml` from `./dags` to `./dags-v2`

6. Stop and Start the Airflow cluster.

7. Check time zone in one of the airflow main containers :  `date`

8. check the airflow UI

9. import `variables.json` in `scripts` folder to resolve the last error . : Admin--> Variables--> Import Variables

   