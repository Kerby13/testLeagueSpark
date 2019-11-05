projectName="test_mr" //название проекта
jarName=test_mr-1.0.0.jar  //название .jar-файла
mainClass="ru.digitalleague.test_mr.system.TestMain" //main-класс

reducers=1 //число редьюсеров

//путь до входного файла 1-ого этапа
firstInputPath="/user/tcld/LNA/test/cdr.csv" 

//путь до 1-ого входного файла 2-ого этапа
second1InputPath="/user/tcld/LNA/test/DIM_BAN.csv" 

//путь до 2-ого входного файла 2-ого этапа
second2InputPath="/user/tcld/LNA/test/DIM_SUBSCRIBER.csv" 

//путь до итогового файла 1-ого этапа
third1InputPath="/user/tcld/LNA/test/output/test_output1.csv" 

//путь до итогового файла 2-ого этапа
third2InputPath="/user/tcld/LNA/test/output/test_output2.csv" 

//путь до папки с результатом
testOutputPath="/user/tcld/LNA/test/output"


// удалям папку с результатом, если она есть
hadoop fs -rm -R ${testOutputPath} 

hadoop jar ${jarName} ${mainClass} \
PROJECT_NAME=${projectName} \
INPUT_SHOP_PATH1=${firstInputPath} \
INPUT_SHOP_PATH2=${firstInputPath} \
OUTPUT_SHOP_PATH=${testOutputPath} \
REDUCES_CNT=${reducers} \
STAGE_NUMBER=${1}

hadoop jar ${jarName} ${mainClass} \
PROJECT_NAME=${projectName} \
INPUT_SHOP_PATH1=${second1InputPath} \
INPUT_SHOP_PATH2=${second2InputPath} \
OUTPUT_SHOP_PATH=${testOutputPath} \
REDUCES_CNT=${reducers} \
STAGE_NUMBER=${2}

hadoop jar ${jarName} ${mainClass} \
PROJECT_NAME=${projectName} \
INPUT_SHOP_PATH1=${third1InputPath} \
INPUT_SHOP_PATH2=${third2InputPath} \
OUTPUT_SHOP_PATH=${testOutputPath} \
REDUCES_CNT=${reducers} \
STAGE_NUMBER=${3}
