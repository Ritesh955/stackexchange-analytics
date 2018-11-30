from pyspark import SparkConf, SparkContext
import sys,re,math,os,gzip,uuid,datetime
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row

spark = SparkSession.builder.appName('StackExchange Data Loader').config("spark.executor.memory", "70g").config("spark.driver.memory", "50g").config("spark.memory.offHeap.enabled",'true').config("spark.memory.offHeap.size","16g").getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
sc = spark.sparkContext

def main(inputs):
    # Read Users table
    users = spark.read.parquet(inputs).repartition(96)
    # Read ISO codes for country
    iso_codes = spark.read.csv("/user/cmandava/countrycodes.csv",header=True).select(functions.col('Name').alias('Country'),functions.col("country-code").alias('Countrycode')).cache()
    
    users.createOrReplaceTempView("users")

    users = spark.sql(" SELECT Reputation, \
           DisplayName AS Name, \
           Id AS UserId, \
          CASE \
                WHEN UPPER(Location) LIKE '%ADELAIDE%' THEN 'Australia' \
                WHEN UPPER(Location) LIKE '%AFGHANISTAN%' THEN 'Afghanistan' \
                WHEN UPPER(Location) LIKE '%AGGIELAND%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%AHEMEDABABD%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%AHMEDABAD%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%AJMER%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%ALABAMA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%ALASKA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%ALBANIA%' THEN 'Albania' \
                WHEN UPPER(Location) LIKE '%ALBERTA%' THEN 'Alberta' \
                WHEN UPPER(Location) LIKE '%ALGERIA%' THEN 'Algeria' \
                WHEN UPPER(Location) LIKE '%ALICANTE%' THEN 'Spain' \
                WHEN UPPER(Location) LIKE '%AMERICAN SAMOA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%AMMAN%' THEN 'Jordan' \
                WHEN UPPER(Location) LIKE '%AMSTERDAM%' THEN 'Netherlands' \
                WHEN UPPER(Location) LIKE '%ANDORRA%' THEN 'Andorra' \
                WHEN UPPER(Location) LIKE '%ANKARA%' THEN 'Turkey' \
                WHEN UPPER(Location) LIKE '%ANTARCTIC%' THEN 'Antarctica' \
                WHEN UPPER(Location) LIKE '%ANTIGUA AND BARBUDA%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%ARGENTINA%' THEN 'Argentina' \
                WHEN UPPER(Location) LIKE '%ARIZONA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%ARKANSAS%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%ARLINGTON VA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%ARMENIA%' THEN 'Armenia' \
                WHEN UPPER(Location) LIKE '%ARUBA%' THEN 'Netherlands' \
                WHEN UPPER(Location) LIKE '%ASTURIES%' THEN 'Spain' \
                WHEN UPPER(Location) LIKE '%AUCKLAND%' THEN 'New Zealand' \
                WHEN UPPER(Location) LIKE '%AUSTRALIA%' THEN 'Australia' \
                WHEN UPPER(Location) LIKE '%AUSTRIA%' THEN 'Austria' \
                WHEN UPPER(Location) LIKE '%AZERBAIJAN%' THEN 'Azerbaijan' \
                WHEN UPPER(Location) LIKE '%BAHAMAS%' THEN 'Bahamas' \
                WHEN UPPER(Location) LIKE '%BAHRAIN%' THEN 'Bahrain' \
                WHEN UPPER(Location) LIKE '%BANGALORE%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%THAILAND%' THEN 'Thailand' \
                WHEN UPPER(Location) LIKE '%BANGLADESH%' THEN 'Bangladesh' \
                WHEN UPPER(Location) LIKE '%BANGLORE%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%BARBADOS%' THEN 'Barbados' \
                WHEN UPPER(Location) LIKE '%BARCELONA%' THEN 'Spain' \
                WHEN UPPER(Location) LIKE '%BARODA%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%BATTARAMULLA%' THEN 'Sri Lanka' \
                WHEN UPPER(Location) LIKE '%BEIJING%' THEN 'China' \
                WHEN UPPER(Location) LIKE '%BELARUS%' THEN 'Belarus' \
                WHEN UPPER(Location) LIKE '%BELGIUM%' THEN 'Belgium' \
                WHEN UPPER(Location) LIKE '%BELIZE%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%BENGALURU%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%BENIN%' THEN 'Benin' \
                WHEN UPPER(Location) LIKE '%BERLIN%' THEN 'Germany' \
                WHEN UPPER(Location) LIKE '%BERMUDA%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%BHILWARA%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%BHOPAL%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%BHUBNESHWAR%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%BHUTAN%' THEN 'Bhutan' \
                WHEN UPPER(Location) LIKE '%BISHOP AUCKLAND%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%BOLIVIA%' THEN 'Bolivia' \
                WHEN UPPER(Location) LIKE '%BOLOGNA%' THEN 'Italia' \
                WHEN UPPER(Location) LIKE '%BOSNIA%' THEN 'Bosnia and Herzegovina' \
                WHEN UPPER(Location) LIKE '%BOSTON%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%BOTSWANA%' THEN 'Botswana' \
                WHEN UPPER(Location) LIKE '%BRASIL%' THEN 'Brazil' \
                WHEN UPPER(Location) LIKE '%BRAZIL%' THEN 'Brazil' \
                WHEN UPPER(Location) LIKE '%BREMEN%' THEN 'Germany' \
                WHEN UPPER(Location) LIKE '%BRIBIE ISLAND%' THEN 'Australia' \
                WHEN UPPER(Location) LIKE '%BRIGHTON & HOVE%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%BRITISH COLUMBIA%' THEN 'Canada' \
                WHEN UPPER(Location) LIKE '%BRUNEI%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%BRUXELLES%' THEN 'Belgium' \
                WHEN UPPER(Location) LIKE '%BUDAPEST%' THEN 'Hungary' \
                WHEN UPPER(Location) LIKE '%BULGARIA%' THEN 'Bulgaria' \
                WHEN UPPER(Location) LIKE '%BURSA%' THEN 'Turkey' \
                WHEN UPPER(Location) LIKE '%CALIFORNIA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%CAMBODIA%' THEN 'Cambodia' \
                WHEN UPPER(Location) LIKE '%CAMBRIDGE,UK%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%CAMEROON%' THEN 'Cameroon' \
                WHEN UPPER(Location) LIKE '%CANADA%' THEN 'Canada' \
                WHEN UPPER(Location) LIKE '%CANTABURY UK%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%CARIBBEAN%' THEN 'Caribbean' \
                WHEN UPPER(Location) LIKE '%CATALONIA%' THEN 'Spain' \
                WHEN UPPER(Location) LIKE '%CAYMAN ISLANDS%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%CHANDIGARH%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%CHANDIGHAR%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%CHANDIGHARH%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%CHARLOTTE%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%CHENNAI%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%CHICAGO%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%CHICOUTIMI%' THEN 'Canada' \
                WHEN UPPER(Location) LIKE '%CHILE%' THEN 'Chile' \
                WHEN UPPER(Location) LIKE '%CHINA%' THEN 'China' \
                WHEN UPPER(Location) LIKE '%CHITWAN%' THEN 'Nepal' \
                WHEN UPPER(Location) LIKE '%CHRISTCHURCH%' THEN 'New Zealand' \
                WHEN UPPER(Location) LIKE '%COLOMBIA%' THEN 'Colombia' \
                WHEN UPPER(Location) LIKE '%COLORADO%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%CONGO%' THEN 'Republic of the Congo' \
                WHEN UPPER(Location) LIKE '%CONNECTICUT%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%CORSICA%' THEN 'France' \
                WHEN UPPER(Location) LIKE '%COSTA RICA%' THEN 'Costa Rica' \
                WHEN UPPER(Location) LIKE '%COUMBIA SC%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%CROATIA%' THEN 'Croatia' \
                WHEN UPPER(Location) LIKE '%CUBA%' THEN 'Cuba' \
                WHEN UPPER(Location) LIKE '%CYPRUS%' THEN 'Cyprus' \
                WHEN UPPER(Location) LIKE '%CZECH%' THEN 'Czech Republic ' \
                WHEN UPPER(Location) LIKE '%DEHRADUN%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%DELAWARE%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%DELHI%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%DENMARK%' THEN 'Denmark' \
                WHEN UPPER(Location) LIKE '%DISTRICT OF COLUMBIA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%DOMBIVLI%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%DOMINICAN REPUBLIC%' THEN 'Dominican Republic' \
                WHEN UPPER(Location) LIKE '%DUBAI%' THEN 'United Arab Emirates' \
                WHEN UPPER(Location) LIKE '%DUBLIN%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%EASTER ISLAND%' THEN 'Chile' \
                WHEN UPPER(Location) LIKE '%ECUADOR%' THEN 'Ecuador' \
                WHEN UPPER(Location) LIKE '%EDINBURGH%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%EGYPT%' THEN 'Egypt' \
                WHEN UPPER(Location) LIKE '%EL SALVADOR%' THEN 'El Salvador' \
                WHEN UPPER(Location) LIKE '%EN_GB%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%ENGLAND%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%ESTONIA%' THEN 'Estonia' \
                WHEN UPPER(Location) LIKE '%ETHIOPIA%' THEN 'Ethiopia' \
                WHEN UPPER(Location) LIKE '%EYGPT%' THEN 'Egypt' \
                WHEN UPPER(Location) LIKE '%FABRIANO%' THEN 'Italy' \
                WHEN UPPER(Location) LIKE '%FAROE ISLANDS%' THEN 'Faroe Islands' \
                WHEN UPPER(Location) LIKE '%FIJI%' THEN 'Fiji' \
                WHEN UPPER(Location) LIKE '%FINLAND%' THEN 'Finland' \
                WHEN UPPER(Location) LIKE '%FRANCE%' THEN 'France' \
                WHEN UPPER(Location) LIKE '%FREMANTLE%' THEN 'Australia' \
                WHEN UPPER(Location) LIKE '%FRENCH RIVIERA%' THEN 'France' \
                WHEN UPPER(Location) LIKE '%GANDHINAGAR%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%GEORGIA%' THEN 'Georgia' \
                WHEN UPPER(Location) LIKE '%GERMANY%' THEN 'Germany' \
                WHEN UPPER(Location) LIKE '%GERMNAY%' THEN 'Germany'                 \
                WHEN UPPER(Location) LIKE '%GHANA%' THEN 'Ghana' \
                WHEN UPPER(Location) LIKE '%GIBRALTAR%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%GOLD COAST%' THEN 'Australia' \
                WHEN UPPER(Location) LIKE '%GREECE%' THEN 'Greece' \
                WHEN UPPER(Location) LIKE '%GREENLAND%' THEN 'Denmark' \
                WHEN UPPER(Location) LIKE '%GUATEMALA%' THEN 'Guatemala' \
                WHEN UPPER(Location) LIKE '%GUELPH%' THEN 'Canada' \
                WHEN UPPER(Location) LIKE '%GURGAON%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%GUYANA%' THEN 'Guyana' \
                WHEN UPPER(Location) LIKE '%HAMBURG%' THEN 'Germany' \
                WHEN UPPER(Location) LIKE '%HAWAI%' THEN 'Hawaii' \
                WHEN UPPER(Location) LIKE '%HO CHI MINH CITY%' THEN 'China' \
                WHEN UPPER(Location) LIKE '%HOBART%' THEN 'Australia' \
                WHEN UPPER(Location) LIKE '%HOLLYWOOD%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%HONDURAS%' THEN 'Honduras' \
                WHEN UPPER(Location) LIKE '%HONG KONG%' THEN 'China' \
                WHEN UPPER(Location) LIKE '%HONGKONG%' THEN 'China' \
                WHEN UPPER(Location) LIKE '%HUNGARY%' THEN 'Hungary' \
                WHEN UPPER(Location) LIKE '%HYDERABAD%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%ICELAND%' THEN 'Iceland' \
                WHEN UPPER(Location) LIKE '%IDAHO%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%ILLINOIS%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%INDIA%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%INDONESIA%' THEN 'Indonesia' \
                WHEN UPPER(Location) LIKE '%INDORE%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%IOWA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%IRAN%' THEN 'Iran' \
                WHEN UPPER(Location) LIKE '%IRAQ%' THEN 'Iraq' \
                WHEN UPPER(Location) LIKE '%IRELAND%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%ISLE OF MAN%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%ISRAEL%' THEN 'Israel' \
                WHEN UPPER(Location) LIKE '%ISTANBUL%' THEN 'Turkey' \
                WHEN UPPER(Location) LIKE '%ITALIA%' THEN 'Italy' \
                WHEN UPPER(Location) LIKE '%RAJASTHAN%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%JAKARTA%' THEN 'Indonesia' \
                WHEN UPPER(Location) LIKE '%JAMAICA%' THEN 'Jamaica' \
                WHEN UPPER(Location) LIKE '%JAMKALYANPUR%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%JAPAN%' THEN 'Japan' \
                WHEN UPPER(Location) LIKE '%JORDAN%' THEN 'Jordan' \
                WHEN UPPER(Location) LIKE '%KAMPALA%' THEN 'Uganda' \
                WHEN UPPER(Location) LIKE '%KAMPOT%' THEN 'Cambodia' \
                WHEN UPPER(Location) LIKE '%KANSAS%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%KARACHI%' THEN 'Pakistan' \
                WHEN UPPER(Location) LIKE '%KASHAN%' THEN 'Iran' \
                WHEN UPPER(Location) LIKE '%KATHMANDU%' THEN 'Nepala' \
                WHEN UPPER(Location) LIKE '%KAZAKHSTAN%' THEN 'Kazakhstan' \
                WHEN UPPER(Location) LIKE '%KENTUCKY%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%KENYA%' THEN 'Kenya' \
                WHEN UPPER(Location) LIKE '%KERALA%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%KHARKOV%' THEN 'Ukraine' \
                WHEN UPPER(Location) LIKE '%KIEV%' THEN 'Ukraine' \
                WHEN UPPER(Location) LIKE '%KOREA%' THEN 'South Korea' \
                WHEN UPPER(Location) LIKE '%KRASNOYARSK%' THEN 'Russia' \
                WHEN UPPER(Location) LIKE '%KUWAIT%' THEN 'Kuwait' \
                WHEN UPPER(Location) LIKE '%KYIV%' THEN 'Ukraine' \
                WHEN UPPER(Location) LIKE '%KYRGYZSTAN%' THEN 'Kyrgyzstan' \
                WHEN UPPER(Location) LIKE '%LAHORE%' THEN 'Pakistan' \
                WHEN UPPER(Location) LIKE '%LATVIA%' THEN 'Latvia' \
                WHEN UPPER(Location) LIKE '%LEBANON%' THEN 'Lebanon' \
                WHEN UPPER(Location) LIKE '%LIBYA%' THEN 'Libya' \
                WHEN UPPER(Location) LIKE '%LIECHTENSTEIN%' THEN 'Liechtenstein' \
                WHEN UPPER(Location) LIKE '%LINDENWOLD NJ%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%LISBON%' THEN 'Portugal' \
                WHEN UPPER(Location) LIKE '%LITHUANIA%' THEN 'Lithuania' \
                WHEN UPPER(Location) LIKE '%LONDON%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%LONG ISLAND%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%LOS ANGELES%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%LOUISIANA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%LUDHIANA%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%LUXEMBOURG%' THEN 'Luxembourg' \
                WHEN UPPER(Location) LIKE '%LYON%' THEN 'France' \
                WHEN UPPER(Location) LIKE '%MACEDONIA%' THEN 'Macedonia' \
                WHEN UPPER(Location) LIKE '%MADURAI%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%MAINE%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%MAKATI CITY%' THEN 'Philippines' \
                WHEN UPPER(Location) LIKE '%MALAYSIA%' THEN 'Malaysia' \
                WHEN UPPER(Location) LIKE '%MALDIVES%' THEN 'Maldives' \
                WHEN UPPER(Location) LIKE '%MALTA%' THEN 'Malta' \
                WHEN UPPER(Location) LIKE '%MANCHESTER%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%MARTINIQUE%' THEN 'France' \
                WHEN UPPER(Location) LIKE '%MARYLAND%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%MASHHAD%' THEN 'Iran' \
                WHEN UPPER(Location) LIKE '%MASSACHUSETTS%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%MAURITANIA%' THEN 'Mauritania' \
                WHEN UPPER(Location) LIKE '%MAURITIUS%' THEN 'Mauritius' \
                WHEN UPPER(Location) LIKE '%MEHRSHAHR%' THEN 'Iran' \
                WHEN UPPER(Location) LIKE '%METAXOURGIO%' THEN 'Greece' \
                WHEN UPPER(Location) LIKE '%MEXICO%' THEN 'Mexico' \
                WHEN UPPER(Location) LIKE '%MICHIGAN%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%MIDS. UK%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%MILAN%' THEN 'Italy' \
                WHEN UPPER(Location) LIKE '%MINNESOTA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%MISSISSIPPI%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%MISSOURI%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%MOLDOVA%' THEN 'Moldova' \
                WHEN UPPER(Location) LIKE '%MONGOLIA%' THEN 'Mongolia' \
                WHEN UPPER(Location) LIKE '%MONTANA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%MONTENEGRO%' THEN 'Montenegro' \
                WHEN UPPER(Location) LIKE '%MONTREAL%' THEN 'Canada' \
                WHEN UPPER(Location) LIKE '%MOROCCO%' THEN 'Morocco' \
                WHEN UPPER(Location) LIKE '%MOSCOW%' THEN 'Russia' \
                WHEN UPPER(Location) LIKE '%MOZAMBIQUE%' THEN 'Mozambique' \
                WHEN UPPER(Location) LIKE '%MULUND%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%MUMBAI%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%MUNICH%' THEN 'Germany' \
                WHEN UPPER(Location) LIKE '%MYANMAR%' THEN 'Myanmar' \
                WHEN UPPER(Location) LIKE '%NAMIBIA%' THEN 'Namibia' \
                WHEN UPPER(Location) LIKE '%NANKUNSHAN%' THEN 'China' \
                WHEN UPPER(Location) LIKE '%NAPOLI%' THEN 'Italy' \
                WHEN UPPER(Location) LIKE '%NASHIK%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%NEBRASKA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%NEPAL%' THEN 'Nepal' \
                WHEN UPPER(Location) LIKE '%NETHERLANDS%' THEN 'Netherlands' \
                WHEN UPPER(Location) LIKE '%NEVADA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%NEW CALEDONIA%' THEN 'New Caledonia' \
                WHEN UPPER(Location) LIKE '%NEW DELHI%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%NEW ENGLAND%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%NEW HAMPSHIRE%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%NEW JERSEY%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%NEW MALDEN%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%NEW MEXICO%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%NEW YORK%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%NEW ZEALAND%' THEN 'New Zealand' \
                WHEN UPPER(Location) LIKE '%NEWCASTLE%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%NICARAGUA%' THEN 'Nicaragua' \
                WHEN UPPER(Location) LIKE '%NIGER%' THEN 'Niger' \
                WHEN UPPER(Location) LIKE '%NIGERIA%' THEN 'Nigeria' \
                WHEN UPPER(Location) LIKE '%NOIDA%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%NORTH AMERICA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%NORTH CAROLINA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%NORTH DAKOTA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%NORTH LOUISIANA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%NORTH RHINE-WESTPHALIA%' THEN 'Germany' \
                WHEN UPPER(Location) LIKE '%NORTH WALES%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%NORWAY%' THEN 'Norway' \
                WHEN UPPER(Location) LIKE '%NOTTINGHAM%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%OHIO%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%OKLAHOMA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%OMAN%' THEN 'Oman' \
                WHEN UPPER(Location) LIKE '%ONTARIO%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%OREGON%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%PAKISTAN%' THEN 'Pakistan' \
                WHEN UPPER(Location) LIKE '%PALESTIN%' THEN 'Palestinian' \
                WHEN UPPER(Location) LIKE '%PANAMA%' THEN 'Panama' \
                WHEN UPPER(Location) LIKE '%PARAGUAY%' THEN 'Paraguay' \
                WHEN UPPER(Location) LIKE '%PARIS%' THEN 'France' \
                WHEN UPPER(Location) LIKE '%PATNA%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%PEMBURY%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%PENANG%' THEN 'Malaysia' \
                WHEN UPPER(Location) LIKE '%PENNSYLVANIA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%PERU%' THEN 'Peru' \
                WHEN UPPER(Location) LIKE '%PETERBOROUGH%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%PHILIPPINES%' THEN 'Philippines' \
                WHEN UPPER(Location) LIKE '%PITTSBURGH%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%POLAND%' THEN 'Poland' \
                WHEN UPPER(Location) LIKE '%PONDICHERRY%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%PORTUGAL%' THEN 'Portugal' \
                WHEN UPPER(Location) LIKE '%PRAGUE%' THEN 'Czech Republic ' \
                WHEN UPPER(Location) LIKE '%PROVIDENCE RI%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%PUERTO RICO%' THEN 'Puerto Rico' \
                WHEN UPPER(Location) LIKE '%PUNE%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%QATAR%' THEN 'Qatar' \
                WHEN UPPER(Location) LIKE '%QUAD CITIES%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%QUEBEC%' THEN 'Canada' \
                WHEN UPPER(Location) LIKE '%RAJKOT%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%RHODE ISLAND%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%ROCHESTER NY%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%ROCKY MOUNTAINS%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%ROMANIA%' THEN 'Romania' \
                WHEN UPPER(Location) LIKE '%ROME%' THEN 'Italy' \
                WHEN UPPER(Location) LIKE '%RUSSIA%' THEN 'Russia' \
                WHEN UPPER(Location) LIKE '%SAI GON - VIET NAM%' THEN 'Vietnam' \
                WHEN UPPER(Location) LIKE '%SAINT-PETERSBURG%' THEN 'Russia' \
                WHEN UPPER(Location) LIKE '%SALEM OR%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%SALT LAKE CITY%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%SAMOA%' THEN 'Samoa' \
                WHEN UPPER(Location) LIKE '%SAN MARINO%' THEN 'San Marino' \
                WHEN UPPER(Location) LIKE '%SANDTON%' THEN 'South Africa' \
                WHEN UPPER(Location) LIKE '%SAO PAULO%' THEN 'Brazil' \
                WHEN UPPER(Location) LIKE '%SAUDI ARABIA%' THEN 'Saudi Arabia' \
                WHEN UPPER(Location) LIKE '%SAUERLAND%' THEN 'Germany' \
                WHEN UPPER(Location) LIKE '%SCOTLAND%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%SENEGAL%' THEN 'Senegal' \
                WHEN UPPER(Location) LIKE '%SERBIA%' THEN 'Serbia' \
                WHEN UPPER(Location) LIKE '%SEYCHELLES%' THEN 'Seychelles' \
                WHEN UPPER(Location) LIKE '%SHANDONG%' THEN 'China' \
                WHEN UPPER(Location) LIKE '%SHANGHAI%' THEN 'China' \
                WHEN UPPER(Location) LIKE '%SIBERIA%' THEN 'Russia' \
                WHEN UPPER(Location) LIKE '%SIERRA LEONE%' THEN 'Sierra Leone' \
                WHEN UPPER(Location) LIKE '%SILICON VALLEY%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%SINCELEJO%' THEN 'Colombia' \
                WHEN UPPER(Location) LIKE '%SINGAPORE%' THEN 'Singapore' \
                WHEN UPPER(Location) LIKE '%SLOVAKIA%' THEN 'Slovakia' \
                WHEN UPPER(Location) LIKE '%SLOVENIA%' THEN 'Slovenia' \
                WHEN UPPER(Location) LIKE '%SOCAL%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%SOLNA%' THEN 'Sweden' \
                WHEN UPPER(Location) LIKE '%SOUTH AFFRICA%' THEN 'South Africa' \
                WHEN UPPER(Location) LIKE '%SOUTH AFRICA%' THEN 'South Africa' \
                WHEN UPPER(Location) LIKE '%SOUTH AFRIKA%' THEN 'South Africa' \
                WHEN UPPER(Location) LIKE '%SOUTH CAROLINA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%SOUTH DAKOTA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%SPAIN%' THEN 'Spain' \
                WHEN UPPER(Location) LIKE '%SRI LANKA%' THEN 'Sri Lanka' \
                WHEN UPPER(Location) LIKE '%SUDAN%' THEN 'Sudan' \
                WHEN UPPER(Location) LIKE '%SURAT%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%SUTTON%' THEN 'Canada' \
                WHEN UPPER(Location) LIKE '%SVALBARD AND JAN MAYEN%' THEN 'Norway' \
                WHEN UPPER(Location) LIKE '%SWANSEA%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%SWAZILAND%' THEN 'Swaziland' \
                WHEN UPPER(Location) LIKE '%SWEDEN%' THEN 'Sweden' \
                WHEN UPPER(Location) LIKE '%SWITZERLAND%' THEN 'Switzerland' \
                WHEN UPPER(Location) LIKE '%SWIZERLAND%' THEN 'Swizerland' \
                WHEN UPPER(Location) LIKE '%SYDNEY%' THEN 'Australia' \
                WHEN UPPER(Location) LIKE '%SYRIA%' THEN 'Syria' \
                WHEN UPPER(Location) LIKE '%TAIWAN%' THEN 'Taiwan' \
                WHEN UPPER(Location) LIKE '%TAJIKISTAN%' THEN 'Tajikistan' \
                WHEN UPPER(Location) LIKE '%TAKANINI%' THEN 'New Zealand' \
                WHEN UPPER(Location) LIKE '%TAMIL NADU%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%TANZANIA%' THEN 'Tanzania' \
                WHEN UPPER(Location) LIKE '%TEHRAN%' THEN 'Iran' \
                WHEN UPPER(Location) LIKE '%TENNESSEE%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%TEXAS%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%THAILAND%' THEN 'Thailand' \
                WHEN UPPER(Location) LIKE '%THAILAND%' THEN 'Thailand' \
                WHEN UPPER(Location) LIKE '%THESSALONIKI%' THEN 'Greece' \
                WHEN UPPER(Location) LIKE '%TIZNIT%' THEN 'Morocco' \
                WHEN UPPER(Location) LIKE '%TOGO%' THEN 'Togo' \
                WHEN UPPER(Location) LIKE '%TORONTO%' THEN 'Canada' \
                WHEN UPPER(Location) LIKE '%TRENTO%' THEN 'Italy' \
                WHEN UPPER(Location) LIKE '%TRINIAD AND TOBAGO%' THEN 'Trinidad and Tobago' \
                WHEN UPPER(Location) LIKE '%TRINIDAD AND TOBAGO%' THEN 'Trinidad and Tobago' \
                WHEN UPPER(Location) LIKE '%TRIVANDRUM%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%TUNISIA%' THEN 'Tunisia' \
                WHEN UPPER(Location) LIKE '%TURKEY%' THEN 'Turkey' \
                WHEN UPPER(Location) LIKE '%TURKIYE%' THEN 'Turkey' \
                WHEN UPPER(Location) LIKE '%TURKMENISTAN%' THEN 'Turkmenistan' \
                WHEN UPPER(Location) LIKE '%TUVALU%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%UDAIPUR%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%UGANDA%' THEN 'Uganda' \
                WHEN UPPER(Location) LIKE '%UKRAINE%' THEN 'Ukraine' \
                WHEN UPPER(Location) LIKE '%UNITED ARAB EMIRATES%' THEN 'United Arab Emirates' \
                WHEN UPPER(Location) LIKE '%UNITED KINGDO%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%UNITED STATES%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%URUGUAY%' THEN 'Uruguay' \
                WHEN UPPER(Location) LIKE '%US VIRGIN ISLANDS%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%USA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%USSR%' THEN 'Russia' \
                WHEN UPPER(Location) LIKE '%UTAH%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%UZBEKISTAN%' THEN 'Uzbekistan' \
                WHEN UPPER(Location) LIKE '%VALENCIA%' THEN 'Spain' \
                WHEN UPPER(Location) LIKE '%VAN NUYS%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%VANCOUVER%' THEN 'Canada' \
                WHEN UPPER(Location) LIKE '%VENEZUELA%' THEN 'Venezuela' \
                WHEN UPPER(Location) LIKE '%VENICE,CA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%VERMONT%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%VIENNA%' THEN 'Austria' \
                WHEN UPPER(Location) LIKE '%VIETNAM%' THEN 'Vietnam' \
                WHEN UPPER(Location) LIKE '%VIJAYAWADA%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%VIRGIN ISLANDS%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%VIRGINIA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%VISHAKAPATNAM%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%VIZAG%' THEN 'India' \
                WHEN UPPER(Location) LIKE '%WALES%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%WARRINGTON%' THEN 'United Kingdom' \
                WHEN UPPER(Location) LIKE '%WARSAW%' THEN 'Poland' \
                WHEN UPPER(Location) LIKE '%WASHING DC METRO AREA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%WEST COAST NORTH AMERICA%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%WISCONAIN%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%WISCONSIN%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%WYOMING%' THEN 'United States' \
                WHEN UPPER(Location) LIKE '%YEMEN%' THEN 'Yemen' \
                WHEN UPPER(Location) LIKE '%ZAMBIA%' THEN 'Zambia' \
                WHEN UPPER(Location) LIKE '%ZHEJIANG%' THEN 'China' \
                WHEN UPPER(Location) LIKE '%ZIMBABWE%' THEN 'Zimbabwe' \
                WHEN UPPER(Location) LIKE '%UK%' THEN 'United Kingdom' \
            ELSE 'Unknown' \
         END AS Country \
      FROM users \
    ")
    
    #Total number of users by country 
    users.createOrReplaceTempView("users_by_country")
    user_base = users.filter(users.Country.isNotNull()).groupBy('country').agg(functions.count('UserId').alias("userCount"))
    user_base = user_base.join(iso_codes,['Country'])
    user_base.show()

    user_base.write.csv('user_base',mode="overwrite")

    reputaion_base = users.filter(users.Country.isNotNull()).groupBy('Country').agg(functions.sum('Reputation').alias("reputation"))
    reputaion_base = reputaion_base.join(iso_codes,['Country'])
    reputaion_base.show()

    reputaion_base.write.csv('reputation_base',mode="overwrite")
    
    #Users with highest reputation by country
    highest_reputation = spark.sql(" select distinct Username,Country,Reputation from (SELECT UserId, \
         Country, \
         Name as Username, \
         RANK() OVER (PARTITION BY Country ORDER BY int(Reputation) desc) AS Rank, \
         Reputation from (select * from users_by_country where Country!='Unknown')) where Rank =1 order by int(Reputation) desc")
    
    highest_reputation.show()

    highest_reputation.write.csv('highest_reputation',mode="overwrite")

    #Best Average Reputation of Top 100 Users by Country
    top100_rep =spark.sql(" select Country,avg(int(Reputation)) as reputation from (SELECT UserId, \
         Country, \
         Name as Username, \
         RANK() OVER (PARTITION BY Country ORDER BY int(Reputation) desc) AS Rank, \
         Reputation from (select * from users_by_country where Country!='Unknown')) where Rank <=100 group by Country order by avg(int(Reputation)) desc")

    top100_rep = top100_rep.join(iso_codes,['Country'])
    top100_rep.show()
    
    top100_rep.write.csv('top100_rep',mode="overwrite")


if __name__ == "__main__":
    inputs = sys.argv[1]
    main(inputs)

#How to run this file?
#spark-submit users_analytics.py /user/cmandava/Users_parquet