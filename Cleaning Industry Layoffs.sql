-- 1. Remove Duplicates

/* 
Partition BY will create partitions where there is a unique combination of values from the listed columns
Row number lists number next to each row for these
By combining these together, we create a table where if the row number > 1 then there is a duplicate combination based on the columns in the partition by function
*/

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging;



-- Create staging table

CREATE TABLE `layoffs_staging` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Insert into new table the row_num partitioned by columns

INSERT INTO layoffs_staging
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs;

-- Now we can delete those duplicates

SET SQL_SAFE_UPDATES = 0;

DELETE
FROM layoffs_staging
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE row_num > 1;



-- 2. Standardizing Data

SELECT company,  (TRIM(company))
FROM layoffs_staging;

-- trimming the company column

UPDATE layoffs_staging
SET company = TRIM(company);

-- Check industries

SELECT DISTINCT industry
FROM layoffs_staging
ORDER BY 1;

-- Standardize crypto industry as there are 3 name variations for this industry
SELECT *
FROM layoffs_staging
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Check location and country

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- remove period '.' after country checking the results before action

SELECT DISTINCT country, TRIM(Trailing '.' FROM country)
FROM layoffs_staging
ORDER BY 1
;

UPDATE layoffs_staging
SET country = TRIM(Trailing '.' FROM country)
WHERE country LIKE 'United States%';

-- Check date and desired format

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging;

-- Change date format

UPDATE layoffs_staging
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Alter column type to date

ALTER TABLE layoffs_staging
MODIFY COLUMN `date` DATE;

-- 3. Remove null and blank fields

-- identify null or blank industry

SELECT *
FROM layoffs_staging
WHERE industry IS NULL
OR industry = '';

-- checking to see if there are other entries for the company which include industry, if so I can use this information to populate the null

SELECT *
FROM layoffs_staging
WHERE company = 'Airbnb';

-- change blanks to nulls before updating them
UPDATE layoffs_staging
SET industry = NULL
WHERE industry = '';

-- Using join to check the table to find where industry is null/blank, and in the same table where the same company has a non null value

SELECT *
FROM layoffs_staging as t1
JOIN layoffs_staging as t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- translating this into an update statement

UPDATE layoffs_staging as t1
JOIN layoffs_staging as t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Bally's Interactive still has a null industry, because there was no other row where industry was populated

SELECT *
FROM layoffs_staging
WHERE company LIKE 'Bally%';

-- Check for nulls in essential columns

SELECT *
FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Deleting all entries with null values in two essential columns (total_laid_off, percentage_laid_off), these will not be of use in our analysis

DELETE
FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- 4. Remove unnecessary columns
-- Drop the row_num column as its no longer being used

ALTER TABLE layoffs_staging
DROP COLUMN row_num;

-- Check table after cleaning

SELECT *
FROM layoffs_staging;

