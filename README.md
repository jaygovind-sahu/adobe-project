# Adobe Search Keyword Performance

Process to calculate the search keyword performance.

## Process Overview

Input is a simple tab separated file which contains what we call 
"hit level data". A hit level record is a single "hit" from a 
visitor on the client's site. Based on the client's implementation, 
several variables can be set and sent to Adobe Analytics for 
deeper analysis.

This Python application reads this hit level data and derives the
search keyword performance - answering the question - how much revenue 
is the client getting from external search engines, such as Google, Yahoo 
and MSN, and which keywords are performing the best based on revenue.

### Approach

The calculate the search keyword performance, we can use below approach:

* Each user can be identified by `ip`.

* Each user journey comprises of events starting from a search, and ending at a purchase.

* A "search" can be identified by checking `referrer` and matching with known search engines.

* A "purchase" can be identified by `event_list` (=1).

* We can enumerate the customer journeys and merge search and purchase data. 

* We can get search engine, and keyword data from `referrer`.

* We can calculate revenue from the data parsed from `product_list`.

* We can group and sort the data as required for reporting purposes.

For the given test data, we get below results:

|search_engine_domain|search_keyword|revenue|
|:-------------------|:-------------|------:|
|google.com          |ipod          |480.0  |
|bing.com            |zune          |250.0  |
|yahoo.com           |cd player     |0.0    |


## CDK Overview

The `cdk.json` file tells the CDK Toolkit how to execute your app.

To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package.

To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following step to activate your virtualenv.

```
$ source .venv/bin/activate
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth
```

To add additional dependencies, for example other CDK libraries, just add
them to your `setup.py` file and rerun the `pip install -r requirements.txt`
command.

### Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

Enjoy!

## Other commands

All tests are under the `tests` folder. You can run all the tests by running:

```
$ pytest tests
```

Below command can be used to clean up compiled python files:
```
$ pyclean .
```

## Using Docker

The `Dockerfile` can be used to build a Docker image and run the application:

```
$ docker build . -t adobe-project
```

The image will contain all the source code, and run all the tests during the build. Below command can be run to run the image created in above step:

```
$ docker run -it -v /<user_home_dir>/.aws:/root/.aws adobe-project 
```

`<user_home_dir>` needs to be replaced to the directory where AWS config is stored.

CDK commands can be run in the container as required.