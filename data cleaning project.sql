-- data cleaning project
SELECT *
FROM layoffs;

-- 1. let's clean data first by removing duplicates
-- 2. standardize the data
-- 3. null values
-- 4. remove coloumn and rows that are unneccessary

-- don't delete a coloumn from a raw dataset meaning when it first begins

-- essentially duplicates the layooffs table headers
CREATE TABLE layoffs_staging
LIKE Layoffs;

SELECT *
FROM layoffs_staging;

-- inserts all the data from layoffs table into layoffs_staging table
INSERT layoffs_staging
SELECT * 
FROM layoffs;

SELECT *
FROM layoffs_staging;

-- we do this since if I make an error, I want to still have the raw data available

-- identify duplicates 
-- Row number function assigns values starting from 1 defined by partition by
-- over defines what interval it works on
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,industry, total_laid_off, percentage_laid_off,'date') AS row_num
FROM layoffs_staging;

-- CTE 
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry, total_laid_off, 
percentage_laid_off, `date`,
stage,country,
funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num>1;
-- can't update the cte so delete doesn't work


CREATE TABLE `layoffs_staging2` (
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


-- inserted a copy of these
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry, total_laid_off, 
percentage_laid_off, `date`,
stage,country,
funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE row_num >1;

DELETE
FROM layoffs_staging2
WHERE row_num >1;

-- standarize data
-- finding errors 

SELECT company, TRIM(company)
FROM layoffs_staging2;

-- taking white spaces off
UPDATE layoffs_staging2
SET company = TRIM(company);

-- fix the industry since crypto, cryptocurrency are the same for all
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- fix a period at the end of a word
SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.'FROM country)
WHERE industry LIKE 'United States%';

-- change date from day/month/year to standard date val
SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y')
;

SELECT *
FROM layoffs_staging2;
-- change date type from text to a date type, never use alter on your original table, its ok on staging table
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- step 3 remove null values

-- must use is null, can't use = null
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry ='';

-- since a company like airbnb has other branches, it will have the same industry, thus not chnaging

UPDATE layoffs_staging2
SET industry =null
WHERE industry ='';
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company =t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry ='')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
    JOIN layoffs_staging2 t2
	ON t1.company =t2.company
SET t1.industry =t2.industry
WHERE (t1.industry IS NULL )
AND t2.industry IS NOT NULL;

-- 4. remove unneccessary rows and coloumns
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- delete row num coloumn
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM layoffs_staging2;


