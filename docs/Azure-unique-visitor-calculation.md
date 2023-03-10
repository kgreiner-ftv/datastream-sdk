<div id="top"></div>

# Azure SDK provisioning for unique visitor calculation

## Deployment using Azure Portal

## Setup Instructions
<p align="left"><a href="#top">Back to Top</a></p>

This document outlines the steps to configure and deploy Python Azure functions for unique visitor calculation


## what is unique visitor?

## A unique visitor calculated every day based on user agent and client IP is an estimate of the number of distinct individuals who visit a website over a period of time. It is typically calculated by analyzing the user agent and client IP address data that is recorded in the website's server logs

## Prerequisite
<p align="left"><a href="#top">Back to Top</a></p>

1. Should have two container created in Azure storageAccount
2. Should have two cosmos database containers created
3. Set <code> aggregation-interval = 86400(seconds)</code> in the configs/provision.json

## Follow below step to setup unique visitor calculation

1. Deploy the datastream-sdk in Azure by following [Deployment using Azure Portal Reference](Azure-portal-deployment.md)

# Run unique visitor
 - Navigate to **Home > Function App**
 - Go to **Functions**
 - Click on **azure_unique_visitor**
 - Click on **Code + Test**
 - Click on **Test/Run**
 - Click on **Input**
 - Select **HTTP method as POST**
 - Select **Key as master (Host Key)**
 - In the **Body** pass the request body as below:-
    ``` 
       {
         "from_date":"YYYY-MM-DD",
         "to_date":"YYYY-MM-DD"  
       }
       Example:-
       {
         "from_date":"2023-02-21",
         "to_date":"2023-02-22"  
       }
   ```
 - Expected response
  ```
      HTTP response code
      200 OK
      {
       "YYYY-MM-DD" : <<unique visitors count>>
      }
    
      Example:-
      {
      "2023-02-22": 11,
      "2023-02-21": 6
      }
  ```
