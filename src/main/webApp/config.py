USER_INFO_PATH = "/Users/youzhenghong/final_project/data/sampled_user_info.csv"
USER_LOG_PATH = "/Users/youzhenghong/final_project/data/sampled_user_log.csv"
TRAINING_SET_PATH = "/Users/youzhenghong/final_project/data/sampled_train_set.csv"
TEST_SET_PATH = "/Users/youzhenghong/final_project/data/sampled_test_set.csv"
CELERY_BROKER_URL = 'redis://localhost:6379/0'
CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'
SPARK_MESSAGE_QUEUE = 'Spark_info_Queue'

SPARK_TEST_COMMAND = 'spark-submit \
            --class "SimpleApp" \
            --master local[4] \
            /Users/youzhenghong/practice/scalasrc/target/scala-2.11/simple-project_2.11-1.0.jar'