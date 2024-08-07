-- /public_power
CREATE OR REPLACE TABLE
  energy_data.public_power(datetime datetime,
    production_type STRING,
    value FLOAT64);

-- /installed_power
CREATE OR REPLACE TABLE
  energy_data.installed_power(year int,
    production_type STRING,
    value FLOAT64);

-- /signal
CREATE OR REPLACE TABLE
  energy_data.signal(datetime datetime,
    share FLOAT64,
    signal STRING);

-- delete table contents (after tests)
DELETE FROM `morning-report-428716.energy_data.installed_power` where 1=1;

DELETE FROM `morning-report-428716.energy_data.public_power` where 1=1;

DELETE FROM `morning-report-428716.energy_data.signal` where 1=1;