SELECT
    cd_gender
FROM customer_demographics
WHERE
    cd_gender = 'M' AND
    cd_marital_status = 'S' AND
    cd_education_status = 'College'
