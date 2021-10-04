# SBCpip


## Overview
`SBCpip` is an R package for processing Stanford Blood Center
data. The data can then be fed to the [Platelet Inventory Prediction
(`pip`)](https://bnaras.github.io/pip) package. 

This separation allows each organization to build appropriate data
processing workflows for local data in site-specific packages and not
worry about the site-independent parts in the `pip` package.

To install `SBCpip`, you need to first install
[`pip`](https://github.com/bnaras/pip) first following instructions
given there.

Then, install `SBCpip` as below.

```{r}
library(devtools)
devtools::install_github("bnaras/SBCpip")
```
## How to use this package
Most of the functionality of this package can be accessed either using a Shiny Dashboard interface or running the appropriate R functions separately. To start the Shiny Dashboard, simply run the following R commands:

```{r}
library(SBCpip)
sbc_dashboard()
```

In either case, the overall predictive workflow for `SBCpip` is as follows:
1. **Gather Data:** Collect appropriate organization-specific data files for all dates of interest in a single directory and complete the provided data-mapping template `data_mapping.csv` (see **Data Preparation** section below).
2. **Build Database:** Set appropriate database configurations and build a DuckDB database with observed variables and responses (platelet usage). This is accomplished using `SBCpip::sbc_build_and_save_full_db()`. This will convert data files to an easily ingestible format and restrict model input to variables of interest (see **Database Structure** section for more details). Configurations can be set either through the Shiny interface or by adjusting the config object (see **Model Configuration** section). 
3. **Adjust Model Settings:** Set model configurations such as training window size, model update frequency, and possible hyperparameters. These configurations can also be set either through the Shiny interface or by adjusting the config object (see **Model Configuration** section).
4. **Validate Model:** Validate the model on previous blood product usage. This will allow the user to evaluate the model's performance on recent data and make any necessary adjustments to model configurations before making projections based on new data. This is accomplished using `SBCpip::sbc_predict_for_range_db`. After predictions have been generated, the user can build a table of inventory levels, waste, and shortage over the validation period using `SBCpip::build_prediction_table()` and obtain summary statistics using `SBCpip::pred_table_analysis()`. Similarly, the user can also build a table of the most prominent features used by the model in making predictions over the validation period using `SBCpip::build_coefficient_table()` and obtain a summary using `SBCpip::coef_table_analysis()`. All of these functions are performed automatically for the user via the Shiny dashboard interface.
5. **Predict for New Data:** Based on observed input data and platelet usage during the specified training window, the model outputs predicted platelet usage for the next [three] days. It then uses current inventory levels to recommend the appropriate number of fresh platelets to collect in [three] days time. 


## Data Preparation
`SBCpip` relies on 5 different file types. Each of the below file types must be added to the same data folder for each observed date:
1. **CBC:** Results of CBC (Complete Blood Count) tests on hospital patients on a given date. The goal is to identify abnormal levels in hospital patients and use these instances as inputs to the model (1 row = 1 unique measurement)
2. **Census:** Locations of patients in the hospital system (e.g. rooms, wards) on a given date. The goal is to identify specific locations that are increasing hospital demand for specific blood products. (1 row = 1 unique patient)
3. **Surgery:** Surgeries carried out by hospital on a given date (or scheduled to be performed in the next 3 days). The goal is to identify particular surgery types that are increasing hospital demand for specific blood products. (1 row = 1 unique operation)
4. **Transfusion:** Transfusions that occur at the hospital on a given date. This is the response which we try to predict (transfusions over next 2 or 3 days) (1 row = 1 unique transfusion of a blood product).
5. **Inventory:** Blood products collected and available in inventory for issue to hospital / transfusions. We use this to compare model performance to actual historical protocol (1 row = 1 unit).

The information contained in each type of file will be organization-specific, so we stipulate essential columns used in data preprocessing. Each organization
should include a table of mappings from their corresponding column headers to the following (we provide a template `sbc_data_mapping.csv` that should be completed):

1. **CBC:**
	* ORDER_PROC_ID: Unique Identifier [Character]
	* BASE_NAME: Specific blood cell / component type. [Character]
	* RESULT_TIME: Datetime at which the test result was obtained. [Datetime ("%d-%b-%y %H:%M:%S")]
	* ORD_VALUE: Value obtained for the specific component as a result of the test [Character - coerced to double]

2. **Census:**
	* PAT_ID: Unique Patient Identifier [Character]
	* LOCATION_DT: Datetime at which patient was logged as present in a specific location/section of the hospital. [Datetime("%m/%d/%Y  %I:%M:%S%p")]
	* LOCATION_NAME: Name of location where patient was present. [Character]

3. **Surgery:**
	* LOG_ID: Unique identifier for set of surgical procedures [Character]
	* SURGERY_DATE: Datetime of procedure [Datetime("%m/%d/%Y  %I:%M:%S %p”)]
	* FIRST_SCHED_DATE: Datetime indicating when procedure was first scheduled [Datetime("%m/%d/%Y  %I:%M:%S %p”)]
	* CASE_CLASS: Indicator of whether surgery is “Elective” or “Urgent” [Character]
	* OR_SERVICE: Specific type of surgery to be carried out. [Character]

4. **Transfusion:**
	* DIN: Donation Identification Number that uniquely identifies the transfused unit [Character]
	* Issue Date/Time: Datetime when component is transfused to patient [Datetime("%m/%d/%Y  %I:%M:%S %p")]
	* Type: Specific blood cell / component type being transfused. We focus on “PLT” [Character]

5. **Inventory:** Blood products collected and available in inventory for issue to hospital / transfusions. We use this to compare model performance to actual historical protocol (1 row = 1 unit).
	* Inv. ID: Unique identifier for product in inventory
	* Type: The specific type of transfusable product (e.g. “PLT”)
	* Days to Expire: Number of days after which unit is considered expired
	* Exp. Date: Specific date on which unit is set to expire

## Model Configuration

## Database Structure


