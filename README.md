# Apache Airflow Best Practices

<a href="https://www.packtpub.com/en-in/product/apache-airflow-best-practices-9781805123750"><img src="https://content.packt.com/_/image/original/B20830/cover_image_large.jpg" alt="no-image" height="256px" align="right"></a>

This is the code repository for [Apache Airflow Best Practices](https://www.packtpub.com/en-in/product/apache-airflow-best-practices-9781805123750), published by Packt.

**A practical guide to orchestrating data workflow with Apache Airflow**

## What is this book about?
With practical approach and detailed examples, this book covers newest features of Apache Airflow 2.x and it's potential for workflow orchestration, operational best practices, and data engineering

This book covers the following exciting features:
* Explore the new features and improvements in Apache Airflow 2.0
* Design and build data pipelines using DAGs
* Implement ETL pipelines, ML workflows, and other advanced use cases
* Develop and deploy custom plugins and UI extensions
* Deploy and manage Apache Airflow in cloud environments such as AWS, GCP, and Azure
* Describe a path for the scaling of your environment over time
* Apply best practices for monitoring and maintaining Airflow

If you feel this book is for you, get your [copy](https://www.amazon.com/Apache-Airflow-Best-Practices-orchestrating/dp/1805123750/ref=sr_1_5?crid=3LFSJQB3N5HQ3&dib=eyJ2IjoiMSJ9.DYT9gh0rBcshIfBk2c3U8dNwIXcULsdC5Evp9ni17gkKdPiXm_KVtyHhvE2sDO98Q9v_ksGkIhi3hku8iiGFlg.BW1m9EkpaBKak3jxVB5UXGUp3GYgv7OShszobesdR3A&dib_tag=se&keywords=Apache+Airflow+Best+Practices&qid=1728622654&sprefix=apache+airflow+best+practices%2Caps%2C303&sr=8-5) today!
<a href="https://www.packtpub.com/?utm_source=github&utm_medium=banner&utm_campaign=GitHubBanner"><img src="https://raw.githubusercontent.com/PacktPublishing/GitHub/master/GitHub.png" 
alt="https://www.packtpub.com/" border="5" /></a>

## Instructions and Navigations
All of the code is organized into folders. For example, chapter-04. 

The code will look like the following:
```
class MetricsPlugin(AirflowPlugin):
    """Defining the plugin class"""
    name = "Metrics Dashboard Plugin"
    flask_blueprints = [metrics_blueprint]
    appbuilder_views = [{
        "name": "Dashboard", "category": "Metrics",
        "view": MetricsDashboardView()
    }]
```

**Following is what you need for this book:**
This book is for data engineers, developers, IT professionals, and data scientists who want to optimize workflow orchestration with Apache Airflow. It's perfect for those who recognize Airflow’s potential and want to avoid common implementation pitfalls. Whether you’re new to data, an experienced professional, or a manager seeking insights, this guide will support you. A functional understanding of Python, some business experience, and basic DevOps skills are helpful. While prior experience with Airflow is not required, it is beneficial.

## To get the most out of this book
The code sources and examples in this book were primarily developed with the assumption that you
would have access to Docker and Docker Compose. We also make some assumptions that you have
a passing familiarity with Python, Kubernetes, and Docker.

With the following software and hardware list you can run all code files present in the book.
### Software and Hardware List
| Software required | OS required |
| ------------------------------------ | ----------------------------------- |
| Airflow 2.0+ | Windows, macOS, or Linux |
| Python 3.9+ | Windows, macOS, or Linux |
| Docker | Windows, macOS, or Linux |
| Postgres | Windows, macOS, or Linux |

We used the [angreal](https://github.com/angreal/angreal) to provide development environments and interactions for consumption. To install `pip install angreal` and then from any folder the following commands will be available for execution as a sub command to angreal. 
(e.g. if you wish to run the demo environment just use `angreal demo`) 

```bash
    demo         commands for controlling the demo environment
    dev-setup    setup a development environment
    help         Print this message or the help of the given subcommand(s)
    test         commands for executing tests
```

## Related products
* Data Engineering Best Practices [[Packt]](https://www.packtpub.com/en-in/product/data-engineering-best-practices-9781803244983) [[Amazon]](https://www.amazon.com/Data-Engineering-Best-Practices-cost-effective/dp/1803244984/ref=sr_1_1?crid=1EESICPK5H9KE&dib=eyJ2IjoiMSJ9.DrrB6027W3PkH39y07z9UUSmzpq9ATt6ETVgHF8MnZ8m6J22loUIArJzetKpZU_0eacquoF9R40TPbDWHAEa3hYTVzhJAsciVw_rf9hXlU2wwVtGtyrGdI-zcy6818Q16Yt5NyKRLTsz0HN1V8_3t14N6O9Uk4z-D0r15SIuawdfDdi6oL5VzQg_tAnVpBRNTzFvKtCGM2yAKsjsGD_7hAsbeQMGFQuF2-opyQLv2Ao.nT4aXTxzhk8CAvmqAwZEMUaBOHqpsfLbwTbwRVkNiiI&dib_tag=se&keywords=Data+Engineering+Best+Practices&qid=1728623158&sprefix=data+engineering+best+practices%2Caps%2C507&sr=8-1)

* In-Memory Analytics with Apache Arrow [[Packt]](https://www.packtpub.com/en-in/product/in-memory-analytics-with-apache-arrow-9781801071031?type=print) [[Amazon]](https://www.amazon.com/Memory-Analytics-Apache-Arrow-hierarchical/dp/1801071039/ref=sr_1_2_sspa?crid=2NRCYXZ97S4U1&dib=eyJ2IjoiMSJ9.NjL2PWzTheZ0pbccXzw42EUD41kQtQHgu1Ff3PeDO2M.mDrcA1L16fo0Rg1Ycfj8Q22Klpa0JUkk60lQC7r_Xrg&dib_tag=se&keywords=In-Memory+Analytics+with+Apache+Arrow&qid=1728623229&sprefix=in-memory+analytics+with+apache+arrow%2Caps%2C834&sr=8-2-spons&sp_csd=d2lkZ2V0TmFtZT1zcF9hdGY&psc=1)

## Get to Know the Authors
**Dylan Intorf**
 is a solutions architect and data engineer with a BS from Arizona State University in Computer Science. He has 10+ years of experience in the software and data engineering space, delivering custom tailored solutions to Tech, Financial, and Insurance industries. 

**Dylan Storey**
 has a B.Sc. and M.Sc. from California State University, Fresno in Biology and a Ph.D. from University of Tennessee, Knoxville in Life Sciences where he leveraged computational methods to study a variety of biological systems. He has over 15 years of experience in building, growing, and leading teams; solving problems in developing and operating data products at a variety of scales and industries.

**Kendrick van Doorn**
 is an engineering and business leader with a background in software development, with over 10 years of developing tech and data strategies at Fortune 100 companies. In his spare time, he enjoys taking classes at different universities and is currently an MBA candidate at Columbia University.
