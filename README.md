# df23_data_loss_prevention
## Sample code from Dreamforce 23 - [Actionable Insights: The Power of Real-Time Events](https://reg.salesforce.com/flow/plus/df23/sessioncatalog/page/catalog/session/1690384804706001a7az)

## Overview
This Python project highlights how to use the Salesforce ReportEventStream Pub/Sub API event to capture potentially malicious access and exports of Salesforce data. While these activities can be prevented using Salesforce Shield Event Monitoring, there are times when it is desirable to allow these data accesses to complete successfully. This demo highlights the fact that the ReportEventStream event provides the exact record IDs of each record that contributes data to a report line. This makes it possible to retrieve the data in near real-time, so that the actual data that was viewed or downloaded can be compressed and saved in the event that forensic analysis is required later.

The main program is formatted_report_event.py. In order to run this, you will need to add valid OAuth and credential information to the credentials.py module.
