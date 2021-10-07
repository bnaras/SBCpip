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

## Main Contents of this Package 

* `R/sbc_globals.R`: Defines the global config object, which contains default values for various model parameters. Includes functions to get and set config values.
* `R/prediction_wrapper.R`: Contains higher level functions that loop over the main functions described in `R/process_data.R` to process data files and generate predictions for a range of dates. Also includes functions that summarize predictions and model coefficients in a tabular format in order to analyze model performance.
* `R/process_data.R`: Contains lower-level functions to preprocess input to the model and generate predictions. In particular, `SBCpip::process_data_for_date_db` reads in the appropriate data files for each date and inserts/updates relevant data to a local DuckDB database. `SBCpip::predict_for_date_db` reads preprocessed data for a range of dates from the database and generates a usage prediction for the next three days.
* `R/webapps.R`: Defines the function `SBCpip::sbc_dashboard()` which initializes the SBCpip Shiny Dashboard. The full dashboard code is contained in `inst/webapps/dashboard/app.R`.
* `R/utilities.R`: Defines common helper functions for the above files.
* `inst/extdata/sbc_data_mapping.csv`: Contains a table which the user must complete to map organization-specific data fields to those utilized by the Stanford Blood Center.

## How to use this package
Most of the functionality of this package can be accessed either using a Shiny Dashboard interface or running the appropriate R functions separately. To start the Shiny Dashboard, simply run the following R commands:

```{r}
library(SBCpip)
sbc_dashboard()
```

In either case, the overall predictive workflow for `SBCpip` is as follows:
1. **Gather Data:** Collect appropriate organization-specific data files for all dates of interest in a single directory and complete the provided data-mapping template `data_mapping.csv` (see **Data Preparation** section below).
2. **Build Database:** Set appropriate database configurations and build a DuckDB database with observed variables and responses (platelet usage). This is accomplished using `SBCpip::sbc_build_and_save_full_db()`. This will convert data files to an easily ingestible format and restrict model input to variables of interest (see **Database Structure** section for more details). Configurations can be set either through the Shiny interface or by adjusting the config object. For parameters such as directory paths that are likely to remain fixed, we recommend updating `sbc_globals.R` directly to avoid updating this information each time (see **Model Configuration** section for more details).
3. **Adjust Model Settings:** Set model configurations such as training window size, model update frequency, and possible hyperparameters. These configurations can also be set either through the Shiny interface or by adjusting the config object (see **Model Configuration** section).
4. **Validate Model:** Validate the model on previous blood product usage. This will allow the user to evaluate the model's performance on recent data and make any necessary adjustments to model configurations before making projections based on new data. This is accomplished using `SBCpip::sbc_predict_for_range_db`. After predictions have been generated, the user can build a table of inventory levels, waste, and shortage over the validation period using `SBCpip::build_prediction_table()` and obtain summary statistics using `SBCpip::pred_table_analysis()`. Similarly, the user can also build a table of the most prominent features used by the model in making predictions over the validation period using `SBCpip::build_coefficient_table()` and obtain a summary using `SBCpip::coef_table_analysis()`. All of these functions are performed automatically for the user via the Shiny dashboard interface.
5. **Predict for New Data:** Based on observed input data and platelet usage during the specified training window, the model outputs predicted platelet usage for the next [three] days. It then uses current inventory levels to recommend the appropriate number of fresh platelets to collect in [three] days time.

## Model Behavior and Configuration

### Model Description and Behavior
On a given day *i*, the model serves to predict blood platelet usage over the following three days, i.e. y<sub>i</sub> = u<sub>i+1</sub> + u<sub>i+2</sub> + u<sub>i+3</sub>. The assumption is that the practitioner must predict on day *i* so that units can be collected and screened prior to availability on day *i+3*.

Predictions are made based on the following features:
1. A moving average of recent platelet usage
2. A binary indicator for each day of the week
3. Hospital features such as Complete Blood Counts (CBC) for patients, number of patients in specific locations of the hospital, and scheduled surgeries (see **`sbc_config`** and **Data Preparation** sections below).

The model is trained using a linear program API (`lpSolve`). The model is a straightforward linear model, but the linear program enforces a number of additional constraints on the resulting coefficients when the model is fit. For example, coefficients must be set such that both waste and shortage generated by the model during the training period are minimized, and the model does not directly penalize for less accurate predictions. This allows the user to specify statistical bias up or down based on the costliness of wasted units vs. product shortages.

The model also constrains the coefficients for hospital features (3) as a form of L1-regularization (LASSO). This constraint (*L*) is a hyperparameter that is tuned via (*n*/14)-fold cross-validation (CV), where *n* is the number of training samples. Due to the temporal arrangement of the data, each evaluation fold is not eliminated from the training set entirely, but rather the optimization problem sets all of the collection amounts during the span of the left-out fold equal to the exact number of platelets used (most efficient case) and ignores any waste and shortage generated during the span of the fold.



### `sbc_config` 
The configuration object, which is assigned to the environment, contains all of the parameters that are relevant to data processing and model training. Most of these are easily configurable from the Shiny Dashboard interface.

#### Directory and File Parameters:
* `data_folder`: The full path to a directory containing all pertinent data files (see **Data Preparation** below), e.g. "/Users/username/Desktop/platelet_prediction/platelet_data/".
* `log_folder`: The full path to a directory that will contain all log files generated by the model, to track usage and any errors.
* `database_path`: The full path to a local DuckDB database file, e.g. "/Users/username/Desktop/platelet_prediction/database.duckdb". The file will be created if it does not already exist. 
* `cbc_filename_prefix`: The string prefix of the daily CBC (Complete Blood Count) reports used as inputs to the model. This prefix should be immediately followed by a date of the form "YYYY-MM-DD" (for example, "LAB-BB-CSRP-CBC_Daily2021-01-01...").
* `census_filename_prefix`: The string prefix of the daily census reports used as inputs to the model. This prefix should be immediately followed by a date of the form "YYYY-MM-DD" (for example, "LAB-BB-CSRP-Census_Daily2021-01-01...").
* `surgery_filename_prefix`: The string prefix of the daily surgery reports used as inputs to the model. This prefix should be immediately followed by a date of the form "YYYY-MM-DD" (for example, "LAB-BB-CSRP-Surgery_Daily2021-01-01...").
* `transfusion_filename_prefix`: The string prefix of the daily blood transfusion reports used as outputs to train the model. This prefix should be immediately followed by a date of the form "YYYY-MM-DD" (for example, "LAB-BB-CSRP-Transfusion_Daily2021-01-01...").
* `inventory_filename_prefix`: The string prefix of the daily inventory reports used as inputs to the model. This prefix should be immediately followed by a date of the form "YYYY-MM-DD" (for example, "Daily_Product_Inventory2021-01-01...").
* `log_filename_prefix`: The string prefix of log files generated by the model when it is run.

#### Model Inputs and Localization
(For more specific details on input data files and required fields, see the *Data Preparation* section below)
* `cbc_quantiles`: A named list of site-specific quantile functions for each CBC of interest
* `cbc_abnormals`: A named list of site-specific functions that flag values as abnormal or not
* `census_locations`: A character vector of locations of interest in the hospital
* `surgery_services`: A character vector of types of surgeries / OR services performed at the hospital, in particular those that typically require platelet transfusions
* `org_cbc_cols`: The columns in the target organization's CBC files that correspond to required fields.
* `org_census_cols`: The columns in the target organization's census files that correspond to required fields.
* `org_surgery_cols`: The columns in the target organization's census files that correspond to required fields.
* `org_transfusion_cols`: The columns in the target organization's transfusion files that correspond to required fields.
* `org_inventory_cols`: The columns in the target organization's inventory files that correspond to required fields.

#### Training Parameters:
* `c0`: The minimal number of fresh platelets remaining at the end of a given day. This is used in model training as a lower bound on number of inventory units expiring in 2 days, and it also serves as the minimum number of new platelets that the blood center should collect on any given day. Increasing this value adds positive bias to the model's predictions.
* `history_window`: The number of previous days to consider as data to train the model for prediction on the next three days. This should be at least 5 times `start` (100 days is typical). Larger history windows tend to result in more conservative (greater) predictions.
* `penalty_factor`: The excess amount which we wish to penalize shortage over waste. For example, if the cost of making up 1 short unit is equivalent to around 15 units wasted, `penalty_factor` = 15.
* `start`: The number of days after the beginning of the model training or evaluation period when we begin to collect new platelets. The actual number of training observations is effectively equal to `history_window` - `start` - 5.
* `initial_collection_data`: The number of blood products initially collected on days `start`, `start + 1`, and `start + 2`.  These are required to initialize model predictions because we need to plan to collect platelets on day i that will be ready for use on day i + 3, so we assume we have already set collection amounts for the first 3 days.
* `initial_expiry_data`: The number of end of day remaining units on day `start` that expire on day `start + 1` and day `start + 2`, respectively.
* `model_update_frequency`: How often we retrain the model (in days), e.g. 7 if we retrain weekly. Model training time depends on the number of `l1_bounds` we consider (see below) as well as the size of `history_window`.
* `lag_window`: As the model is autoregressive (takes previous outputs as input), this controls the number of prior days over which we average usage (default 7 days).
* `l1_bounds`: A sequence of hyperparameter values that control the weight given to the input features aside from day of the week and previous usage (via L1 regularization). A bound of 0 corresponds to no additional input features used. A vector of possible bounds from 0 to 60 is typically sufficient.
* `lag_bounds`: While we assume previous blood product usage level is a key predictor of future usage, this allows the model to restrict the amount of weight given to this variable in favor of others (e.g. when there are abrupt changes in usage that may be caused by or at least correlate with hospital data).

## Data Preparation
`SBCpip` relies on 5 different file types. Each of the below file types must be added to the same data folder for each observed date:
1. **CBC:** Results of CBC (Complete Blood Count) tests on hospital patients on a given date. The goal is to identify abnormal levels in hospital patients and use these instances as inputs to the model (1 row = 1 unique measurement)
2. **Census:** Locations of patients in the hospital system (e.g. rooms, wards) on a given date. The goal is to identify specific locations that are increasing hospital demand for specific blood products. (1 row = 1 unique patient)
3. **Surgery:** Surgeries carried out by hospital on a given date (or scheduled to be performed in the next 3 days). The goal is to identify particular surgery types that are increasing hospital demand for specific blood products. (1 row = 1 unique operation)
4. **Transfusion:** Transfusions that occur at the hospital on a given date. This is the response which we try to predict (transfusions over next 2 or 3 days) (1 row = 1 unique transfusion of a blood product).
5. **Inventory:** Blood products collected and available in inventory for issue to hospital / transfusions. We use this to compare model performance to actual historical protocol (1 row = 1 unit).

The information contained in each type of file will be organization-specific, so we stipulate essential columns used in data preprocessing. Each organization
should include a table of mappings from their corresponding column headers to the following (we provide a template `inst/extdata/sbc_data_mapping.csv` that should be completed):

1. **CBC:** (csv/tsv format)
	* ORDER_PROC_ID: Unique Identifier [Character]
	* BASE_NAME: Specific blood cell / component type. [Character]
	* RESULT_TIME: Datetime at which the test result was obtained. [Datetime ("%d-%b-%y %H:%M:%S")]
	* ORD_VALUE: Value obtained for the specific component as a result of the test [Character - coerced to double]

2. **Census:** (.csv/.tsv format)
	* PAT_ID: Unique Patient Identifier [Character]
	* LOCATION_DT: Datetime at which patient was logged as present in a specific location/section of the hospital. [Datetime("%m/%d/%Y  %I:%M:%S%p")]
	* LOCATION_NAME: Name of location where patient was present. [Character]

3. **Surgery:** (.csv/.tsv format)
	* LOG_ID: Unique identifier for set of surgical procedures [Character]
	* SURGERY_DATE: Datetime of procedure [Datetime("%m/%d/%Y  %I:%M:%S %p”)]
	* FIRST_SCHED_DATE: Datetime indicating when procedure was first scheduled [Datetime("%m/%d/%Y  %I:%M:%S %p”)]
	* CASE_CLASS: Indicator of whether surgery is “Elective” or “Urgent” [Character]
	* OR_SERVICE: Specific type of surgery to be carried out. [Character]

4. **Transfusion:** (.csv/.tsv format)
	* DIN: Donation Identification Number that uniquely identifies the transfused unit [Character]
	* Issue Date/Time: Datetime when component is transfused to patient [Datetime("%m/%d/%Y  %I:%M:%S %p")]
	* Type: Specific blood cell / component type being transfused. The code assumes that "PLT" is contained in the set of possible values [Character]

5. **Inventory:** (.xls format)
	* Inv. ID: Unique identifier for product in inventory [Character]
	* Type: The specific type of transfusable product (e.g. “PLT”) [Character]
	* Days to Expire: Number of days after which unit is considered expired [Double]
	* Exp. Date: Specific date on which unit is set to expire [Datetime("%m/%d/%Y  %I:%M:%S %p")]
	* Exp. Time: Specific time at which unit is set to expire [Double]



## Database Structure



