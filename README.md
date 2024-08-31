

## Project Title # pinterest-data-pipeline898


## **Overview**
This project is designed to send data from an RDS database to Amazon Kinesis Data Streams via an API Gateway. The script reads data from the database and sends it to three different Kinesis streams corresponding to Pinterest data, geolocation data, and user data.

## **Prerequisites**
Before you begin, ensure that you have the following installed:
- **Python 3.7 or later**: [Download Python](https://www.python.org/downloads/)
- **IAM Permissions**: Ensure your IAM user has the necessary permissions to access Kinesis, API Gateway, and other AWS services.

## **Installation**

### **1. Clone the Repository**
Clone the repository to your local machine


### **3. Set Up Database Credentials**
Create a `db_creds.yaml` file in the same directory as the script with the following structure:
```yaml
HOST: 'your-database-host'
USER: 'your-database-username'
PASSWORD: 'your-database-password'
DATABASE: 'your-database-name'
PORT: 3306
```

the current yaml can be given by contacting the creator


## **Usage**


### **1. Run the Script**
To start the script, use the following command:
```bash
python user_posting_emulation.py
```

### **2. Monitor the Output**
- The script continuously reads from the database and sends data to the appropriate Kinesis streams via the API Gateway.
- It prints the status of each API call, showing whether the data was successfully sent.



