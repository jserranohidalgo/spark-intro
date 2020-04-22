## Workflow-example on EMR

### 0. Create AWS account

You can use your personal AWS account, or apply for an AWS Educate account [here](https://aws.amazon.com/es/education/awseducate/).

Some useful links to start with AWS:
* [AWS Cloud Practitioner Essentials (Second Edition)](https://www.aws.training/Details/Curriculum?id=27076&scr=path-cp)
* [Data Analytics Fundamentals](https://www.aws.training/Details/eLearning?id=35364)
* [Introduction to Amazon Elastic MapReduce (EMR)](https://www.aws.training/Details/Video?id=16023)

### 1. Install AWS cli

Follow these [instructions](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html), and configure your [credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).

For AWS Educate accounts, credentials for AWS CLI can be found on "Account Details" at the right panel. Copy & paste those credentials into your `~/.aws/credentials` file (if they expire, just renew them). If you have several AWS profiles, just choose a new name for the profile (instead of "default") and use that name in your AWS commands using `aws ... -profile <profileName>`.

### 2. Upload data to S3

First, create your bucket in S3:

```
aws s3 mb s3://<my-bucket>
```

Note that the name of your bucket must be unique across all S3 users. Also, you may need to explicitly state the `region` and `--endpoint-url`:

```
aws s3 mb s3://<my-bucket> --region us-east-1 --endpoint-url https://s3.us-east-1.amazonaws.com
```

Then, upload your data with the `cp` command. In order to upload data folders use the `--recursive` option:

```
aws s3 cp --recursive data/la-liga.parquet s3://<my-bucket>/data/la-liga.parquet
```

### 3. Upload application jar

First set the input and output parameters in the `workflow-example/src/main/resources/application.conf` file, according to your S3 configuration:

```
input-path="s3://<my-bucket>/data/la-liga.parquet"
output-path="s3://<my-bucket>/data/goalsPerTeam.parquet"
```

Then, generate the jar with the pipeline to be run:

```bash
sbt clean assembly
```

and upload it to S3:

```
aws s3 cp target/scala-2.11/spark-jupyter-assembly-0.0.1-SNAPSHOT.jar s3://<my-bucket>/jars/workflow-example-assembly-0.0.1-SNAPSHOT.jar
```

### 4. Launch an EMR cluster

In the [EMR console](https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1), click on "Create cluster" and choose the software configuration with Spark. Then, set the desired features (number and type of instances, etc.) and click on "Create". The new cluster should be accessible from the EMR console in a few minutes.

If you are not using an starter account, set also the EC2 key-pair to an existing value (which you should have configured previously in the [EC2 console](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html)). This will allow you to enable the Web interfaces (including the Spark UI) following [these instructions](https://aws.amazon.com/es/premiumsupport/knowledge-center/view-emr-web-interfaces/).

### 5. Execute the pipeline in the EMR cluster

To launch the pipeline on the EMR cluster we have to add an step to the cluster. You can use the following script, passing the cluster identifier and your bucket:
```bash
scripts/runpipeline.sh <my-instance> <my-bucket>
```

The resulting activity can be inspected through the Spark History Server (there is a link in the cluster page to open it).

## 6. Jupyter notebooks on EMR

Notebooks attached to the EMR cluster can be created through the cluster page. When the notebook is ready you can open the classical Jupyter or the JupyterLab interfaces.
The kernel used for Scala is named "spark" and it uses [sparkmagic](https://github.com/jupyter-incubator/sparkmagic). In the tab "Application History" of the cluster page, you can obtain information about the jobs, stages and tasks executed through the notebook.

