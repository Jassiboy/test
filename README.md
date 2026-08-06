Dear Team,

We are excited to announce the successful onboarding of the new **CDL ETL Framework**, our cloud-native, metadata-driven ETL solution for building and managing CDL data layers.

As part of this initiative, we have successfully onboarded the **Ground OPCO** solution, with the **Facility, Country, and Global** layers now built entirely on the cloud using this framework. This marks a significant step forward from the traditional implementation by providing a scalable, configurable, and maintainable platform for data processing.

### Key Features

The CDL ETL Framework has been designed to simplify operations, improve reliability, and accelerate onboarding of new data pipelines. Its key capabilities include:

* Audit Balance Control (ABC) framework implementation
* Metadata-driven ETL architecture, where a single configuration table drives the entire load process end-to-end
* No hard-coded pipeline logic for individual tables
* End-to-end source lineage and traceability
* Built-in Data Quality (DQ) framework with configurable validation rules
* Flexible incremental load cutoffs
* Comprehensive audit and monitoring logs with granular execution details
* Robust retry mechanisms for improved fault tolerance and reliability
* Fully parameterized, configuration-driven processing
* Stage table support for testing and controlled deployments
* Flexible load configurations to support multiple ingestion scenarios
* Cloud-agnostic architecture, allowing the framework to be migrated between cloud platforms such as Azure and Google Cloud Platform with minimal effort

### Performance and Cost Comparison

The following slides provide a comparison between the traditional implementation and the new CDL ETL Framework, highlighting improvements in both **load performance** and **overall cost optimization**.

### Up Next

The next phase of the framework includes:

* Onboarding the remaining OPCOs, including **International, Domestic, and TNT**
* Deploying the framework on **Google Cloud Platform (GCP)** as a pilot implementation, leveraging the same metadata-driven and cloud-agnostic architecture
* Enabling **daily incremental processing**, including support for the **Account layer**, to deliver a fully automated end-to-end incremental data pipeline

This framework establishes a strong foundation for building scalable, standardized, and cloud-native ETL solutions while significantly reducing development effort, operational overhead, and future onboarding time.

Finally, a special thanks to **Divyansh** and **Long** for their unwavering support, valuable guidance, and continuous collaboration throughout the design and development of this framework. Their contributions were instrumental in making this initiative a success.

We look forward to onboarding additional solutions and further evolving the framework in the coming phases.

Best Regards,

[Your Name]
