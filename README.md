
# Streaming Data Pipeline for AIS Data
Maritime vessel path trajectory prediction with AIS data has been researched for applications of collision avoidance systems; however, it remains a challenging problem since there are a lot of factors contributing to vessel path decisions. Factors such as weather, fuel consumption, and encounters with adversarial vessels (piracy) influence the vessel\'s path over the course of a voyage. Furthermore, it is difficult to apply path prediction in real-time as most existing approaches are computationally expensive and take several minutes to output predictions. This project will demonstrate real-time ingestion of AIS data from thousands of maritime vessels using:
 - Apache Kafka for storing parsed AIS data, preprocessed features, and model predictions
 - Apache Spark Structured Streaming for filtering the AIS data streaming, creating a feature set for the model and then feeding input data to the model
 - Spark MLLib for modeling path trajectories

# Contents

- [The Data Set](#the-data-set)
- [Used Tools](#used-tools)
  - [Connect](#connect)
  - [Buffer](#buffer)
  - [Processing](#processing)
  - [Storage](#storage)
  - [Visualization](#visualization)
- [Pipelines](#pipelines)
  - [Stream Processing](#stream-processing)
    - [Storing Data Stream](#storing-data-stream)
    - [Processing Data Stream](#processing-data-stream)
  - [Batch Processing](#batch-processing)
  - [Visualizations](#visualizations)
- [Demo](#demo)
- [Conclusion](#conclusion)
- [Follow Me On](#follow-me-on)
- [Appendix](#appendix)


# The Data Set

- Explain the data set
- Why did you choose it?
- What do you like about it?
- What is problematic?
- What do you want to do with it?

| column name  | data type  | description |
| ------------ | ------------ |------------ |
| timestamp_utc | timestamp  |The timestamp of the AIS message transmission|
| mmsi |  string |Mobile Maritime Service Identity. <br></brUsed>Used to uniquely identify AIS data transceiver |
| position |  string \| geospatial |Well-Known Text (WKT) representation of vessel position. Will always be a POINT geometry in WGS84|
| navigation_status | string  |Describes current movement status of vessel|
| speed_over_ground | float  |The current speed of the vessel (units: Knots)|
| course_over_ground |  float |The current speed of the vessel (units: Knots)|
| message_type |  int |The AIS message type. This dataset has been pre-filtered to position history related messages (1,2,3,18,27). See https://arundaleais.github.io/docs/ais/ais_message_types.html for more information|
| source_identifier | string  |N/A|
| position_verified | int  |N/A|
| position_latency | int  |Indicator variable where 0 is if the reported position took less than 5 seconds to receive and is 1 if the reported position took more than 5 second to receive. Only used in message type 27 for long-range detection of AIS messages|
| raim_flag |int|Receiver autonomous integrity monitoring. Boolean indicator used to identify if RAIM process is available. See https://www.navcen.uscg.gov/?pageName=AISMessagesA#RAIM|
| vessel_name |string|Name of the vessel|
| vessel_type |string|Vessel type classification by AIS standards. See https://api.vesselfinder.com/docs/ref-aistypes.html|
| timestamp_offset_seconds |int|UTC second from timestamp_utc field (units: seconds)|
| true_heading |float|The current magnetic heading of the vessel’s bow (units: degrees)|
| rate_of_turn |float|Rate of turn value stored in AIS message; **NOT in deg/min**. Have to divide by 4.733 then square the value in order to convert to degrees/min. See https://faq.spire.com/how-to-convert-the-decoded-rate-of-turn-rot-value-into-degrees-per-minute|
| repeat_indicator |int|Directive to an AIS transceiver that this message should be rebroadcast because of nearby obstructions that can block the message transmission to the ground station.|


# Used Tools
- Explain which tools do you use and why
- How do they work (don't go too deep into details, but add links)
- Why did you choose them
- How did you set them up

## Connect
## Buffer
## Processing
## Storage
## Visualization

# Pipelines
- Explain the pipelines for processing that you are building
- Go through your development and add your source code

## Stream Processing
### Storing Data Stream
### Processing Data Stream
## Batch Processing
## Visualizations

# Demo
- You could add a demo video here
- Or link to your presentation video of the project

# Conclusion
Write a comprehensive conclusion.
- How did this project turn out
- What major things have you learned
- What were the biggest challenges

# Follow Me On
[LinkedIn](https://www.linkedin.com/in/alex-hall-73534515a/ "LinkedIn | Alex Hall")

# Appendix

[Markdown Cheat Sheet](https://github.com/adam-p/markdown-here/wiki/Markdown-Cheatsheet)
