# Connect Power BI to AWS Athena

This guide walks you through setting up **Power BI Desktop** to query data directly from **Amazon Athena** using **ODBC**.

---

##  Prerequisites

- Power BI Desktop (Installed & Signed In)
- AWS Account with IAM permissions
- S3 bucket for Athena query results
- Amazon Athena ODBC Driver

---

## Step 1: Install Power BI Desktop

1. Download and install Power BI Desktop from:  
     https://powerbi.microsoft.com/desktop/
2. Open Power BI and sign in with your Microsoft account.

---

## Step 2: Generate AWS Access Keys

1. Go to [AWS IAM Console](https://console.aws.amazon.com/iam/).
2. Navigate to:  
   `Users` → *[Your IAM User]* → `Security credentials` tab.
3. Click **Create access key**.
4. Copy and securely save:
   - `Access Key ID`
   - `Secret Access Key`

---

## Step 3: Install Athena ODBC Driver

1. Download the ODBC Driver for Amazon Athena:  
     https://docs.aws.amazon.com/athena/latest/ug/athena-odbc.html
2. Choose the version suitable for your OS and install it with default settings.

---

## Step 4: Configure ODBC Data Source (DSN)

1. Open **ODBC Data Sources (64-bit)** from the Start Menu.
2. Go to the **System DSN** tab.
3. Click **Add** → Choose **Amazon Athena ODBC Driver** → Click **Finish**.
4. Fill in the following:
   - **Data Source Name (DSN)**: `Athena-PowerBI`
   - **AWS Region**: `us-east-1` *(or your region)*
   - **S3 Output Location**: `s3://your-query-results-bucket/`
   - **Authentication Type**: `IAM Credentials`
   - **Username**: *Paste Access Key ID*
   - **Password**: *Paste Secret Access Key*

Click **Test** to ensure the connection is successful, then **OK**.
![launch odbc](images/odbc.png)
![add](images/athenaODBC)
![auth](images/ODBCauth)
---

## Step 5: Connect Power BI to Athena

1. Open **Power BI Desktop**.
2. Click **Home** → **Get Data** → **ODBC**.
3. Choose the DSN: `Athena-PowerBI` → Click **OK**.
4. Select your Athena database and table.
5. Click **Load** or **Transform Data**.
![PowerBI](images/powerbiTwo.png)
![PowerBI](images/powerbiOne.png)
---

## You're All Set!

You can now create reports and dashboards in Power BI using live data from Amazon Athena.

> Tip: You can refresh your dataset from Power BI manually or schedule updates

---

##  Notes

- Make sure your S3 output bucket is in the same region as Athena.
- If using federated identity or SSO, consider using temporary credentials or AWS CLI profiles.

---

## Support

If you face issues with ODBC or connection:
- Check AWS IAM permissions for Athena and S3 access.
- Ensure your machine firewall or proxy isn’t blocking ODBC connections.
- Consult AWS Docs or Power BI Forums.

---

## Custimizations you can do:
1. You can use Quicksight, an aws reporting tool, much easy approach if you have access. You just need to select the Athena Source and You are all set
2. Make the reporting responsive to changes (like filters)

