#!/bin/bash
########################################################################################################################
#######This shell script splits the data file into smaller files based on # of lines 				########
#######################                  This shell script takes following inputs:              ########################
########################################################################################################################
##01. Data directory name 											########
##02. Data file name 												########
##03. Data file type (extension) 										########
##04. Number of lines, data file needs to be splitted 								########
##05. Log file directory name 											########
########################################################################################################################

## Validate input parameters, Check if the correct number of arguments has been provided
if [ $# != 5 ]
then
	echo "Usage: Please provide following five arguments:"
	echo "	1. Data Directory name."
	echo "	2. Data File name."
	echo "	3. Data file type (extension)."
	echo "	4. Split number of lines."
	echo "	5. Log file directory name."
	exit 1;
else
	## Set the variables
	export DATA_DIR=$1
	export DATA_FILE_NAME=$2
	export DATA_FILE_TYPE=$3
	if [[ $4 = *[!0-9]* ]]
	then
		echo "Provide a number for splitting the files based on rows. Your input is : $4"
		exit 1;
	else
	export SPLIT_NUM_OF_LINE=$4
	fi
	export LOG_DIR=$5
fi

###### Define Log file
export LOG_FILE_NAME=`basename $0|cut -d "." -f1`
export LOG_FILE=$LOG_DIR/${LOG_FILE_NAME}.log
find ${LOG_DIR} -name ${LOG_FILE_NAME}.log -size +5M -exec rm {} \;
if [ ! -f $LOG_FILE ]
then
	touch ${LOG_FILE}
	chmod 777 ${LOG_FILE}
fi

## Define any functions used in the script
## This function can be called after any command to check if that command completed successfult or not.
check_failure()
{
if [ $? -ne 0 ]
then
	echo "Exiting due to error, for details please check the log file." >> $LOG_FILE
	exit 1;
fi
}

echo "##################################################################################################">>$LOG_FILE
echo "############ `basename $0` started on - `date`  ############">>$LOG_FILE
echo "##################################################################################################">>$LOG_FILE

## Check if the data file exists
if [ ! -f $DATA_DIR/${DATA_FILE_NAME}.${DATA_FILE_TYPE} ]
then
	echo "Data file $DATA_DIR/${DATA_FILE_NAME}.${DATA_FILE_TYPE} does not exists" >> $LOG_FILE
	exit 1;
fi

## Check if the Directory exists, if yes then remove the directory and then recreate it to ensure no other files are no files there
if [ -d $DATA_DIR/$DATA_FILE_NAME ]
then
	echo "Directory $DATA_FILE_NAME already exists, removing existing one and re-creating again." >> $LOG_FILE
	rm -rf $DATA_DIR/$DATA_FILE_NAME >> $LOG_FILE
	check_failure;
	mkdir -m755 $DATA_DIR/$DATA_FILE_NAME >> $LOG_FILE
	check_failure;
else
	echo "Directory $DATA_FILE_NAME does not exists, creating directory." >> $LOG_FILE
	mkdir -m755 $DATA_DIR/$DATA_FILE_NAME >> $LOG_FILE
	check_failure;
fi

echo "Splitting file $DATA_DIR/${DATA_FILE_NAME}.${DATA_FILE_TYPE} into $SPLIT_NUM_OF_LINE line files." >> $LOG_FILE

## Split the file
split -d -l $SPLIT_NUM_OF_LINE $DATA_DIR/${DATA_FILE_NAME}.${DATA_FILE_TYPE} $DATA_DIR/$DATA_FILE_NAME/${DATA_FILE_NAME}.${DATA_FILE_TYPE}. >> $LOG_FILE
check_failure;
rm -f $DATA_DIR/${DATA_FILE_NAME}.${DATA_FILE_TYPE} >> $LOG_FILE
check_failure;
exit 0;
