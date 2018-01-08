# *- coding: utf-8 -*-
# version : v1.0.0
# 需要安装第三方库oss2(pip3 install oss2)

import argparse
import os
import shutil
import oss2
import datetime
import calendar
import json
import time
import sys


# 三个访问的bucket
audio = "ffasr-audio-recall"
decode = "ffasr-decode-recall"
error = "ffasr-decode-err"

# 设置bucket对象的参数
access_key_id = os.getenv('OSS_TEST_ACCESS_KEY_ID', 'LTAIVYM9anNa4pxl')
access_key_secret = os.getenv(
    'OSS_TEST_ACCESS_KEY_SECRET', 'tFSRDV6jy9gBn37xrIYg5f1FLv7wdM')
bucket_audio = os.getenv('OSS_TEST_BUCKET', audio)
bucket_decode = os.getenv('OSS_TEST_BUCKET', decode)
bucket_error = os.getenv('OSS_TEST_BUCKET', error)
endpoint = os.getenv('OSS_TEST_ENDPOINT',
                     'https://oss-cn-beijing.aliyuncs.com')


def setParse():
    # 设置交互参数
    parse = argparse.ArgumentParser(
        usage="PROG [option] <access_id>|<file_full_name>")
    parse.add_argument("-d", "--download", action="store_true",
                       default=False, help="表示是否需要下载,没有直接打印到屏幕")
    parse.add_argument("-t", "--time", dest="STIME", action="store", default=None,
                       help="起始日期/月份,如果没有输入结束日期/月份，将处理这个时间点，如果输入结束日期/月份，将处理这个时间段，Description: YYYY-MM-DD/YYYY-MM")
    parse.add_argument("-e", "--end", dest="ETIME", action="store", default=None,
                       help="结束日期/月份(可选),如果输入的是起始日期，请输入结束日期，如果输入的是结束日期，请输入结束月份，Description: YYYY-MM-DD/YYYY-MM")
    parse.add_argument("-o", "--output", dest="PATH", action="store", default="./",
                       help="出现-d才有效,如果目录为空，数据将直接下载到该目录，如果目录不为空，数据将下载到该目录的oss_dump文件夹中,Description: output directory")
    parse.add_argument("-c", "--count", action="store_true", default=False,
                       help="统计个数,如果有这个的话，-d就无效了")
    parse.add_argument("-err", "--error", action="store_true", default=False,
                       help="如果加上-err，只会在无效音频中查找")
    parse.add_argument("FILE", action="store",
                       help="FILE为all时表示所有用户,自动判别是access_id还是file_full_name")
    global args
    args = parse.parse_args()


def setBucket():
    # 确认相关参数是否填写正确
    global bucketAudio
    global bucketDecode
    global bucketError
    for param in (access_key_id, access_key_secret, bucket_audio, bucket_decode, endpoint):
        assert '<' not in param, '请设置参数：' + param
    # 创建Bucket对象，所有Object相关的接口都可以通过Bucket对象来进行
    bucketAudio = oss2.Bucket(
        oss2.Auth(access_key_id, access_key_secret), endpoint, bucket_audio)
    bucketDecode = oss2.Bucket(
        oss2.Auth(access_key_id, access_key_secret), endpoint, bucket_decode)
    bucketError = oss2.Bucket(
        oss2.Auth(access_key_id, access_key_secret), endpoint, bucket_error)


def getTime(tmp):
    # 转换为标准日期类
    return datetime.datetime.strptime(tmp, "%Y-%m-%d")


def getFirstDay(tmp):
    # 得到月份的第一天
    tmp += "-01"
    return datetime.datetime.strptime(tmp, "%Y-%m-%d")


def getLastDay(tmp):
    # 得到月份的最后一天
    tmpData = datetime.datetime.strptime(tmp, "%Y-%m")
    firstDayWeekDay, monthRange = calendar.monthrange(
        tmpData.year, tmpData.month)
    return getFirstDay(tmp) + datetime.timedelta(days=(monthRange - 1))


def buildTimeList(beginDate, endDate):
    # 生成两个日期/月份之间的所有日期
    global dateList
    dateList = []
    while beginDate <= endDate:
        dateStr = beginDate.strftime("%Y-%m-%d")
        dateList.append(dateStr)
        beginDate += datetime.timedelta(days=1)
    return None


def checkTIME():
    # 检查TIME参数是否正确
    if (args.STIME == None) and (args.ETIME == None):
        # 没有输入时间
        return 0
    elif (args.STIME == None):
        return -1
    elif (args.ETIME == None):
        return 1
    elif (len(args.STIME) != len(args.ETIME)):
        return -1
    else:
        if (len(args.STIME) > 7):
            timeS = getTime(args.STIME)
            timeE = getTime(args.ETIME)
        else:
            timeS = getFirstDay(args.STIME)
            timeE = getLastDay(args.ETIME)
        if (timeS > timeE):
            return -1
        return 2
    return None


def checkFileExist(tmpFile):
    # 检查bucket上某个文件是否存在
    for dateT in dateList:
        if bucketAudio.object_exists(dateT + "/" + tmpFile) == True:
            return True
    return False


def checkFile():
    # 判断file是access_id还是file_full_name
    global all_files
    global access_id
    global file_full_name
    all_files = access_id = file_full_name = False
    if (args.FILE == "all"):
        all_files = True
    elif (checkFileExist(args.FILE) == True):
        file_full_name = True
    else:
        access_id = True
    return None


def prepare():
    # 预处理
    timeStatus = checkTIME()
    if (timeStatus == 0):
        print("Lack Of Time")
        return False
    elif (timeStatus == -1):
        print("Invalid timeme")
        return False
    elif (timeStatus == 1):
        if (len(args.STIME) > 7):
            timeS = getTime(args.STIME)
            timeE = getTime(args.STIME)
        else:
            timeS = getFirstDay(args.STIME)
            timeE = getLastDay(args.STIME)
        buildTimeList(timeS, timeE)
    elif (timeStatus == 2):
        if (len(args.STIME) > 7):
            timeS = getTime(args.STIME)
            timeE = getTime(args.ETIME)
        else:
            timeS = getFirstDay(args.STIME)
            timeE = getLastDay(args.ETIME)
        buildTimeList(timeS, timeE)
    checkFile()
    return True


def countErr():
    # 统计错误音频个数
    countError = 0
    for dateT in dateList:
        if (all_files == True):
            pathT = dateT + "/"
            for obj in oss2.ObjectIterator(bucketError, prefix=pathT):
                if (obj.key == pathT):
                    continue
                countError += 1
        elif (access_id == True):
            pathT = dateT + "/" + args.FILE + "_"
            for obj in oss2.ObjectIterator(bucketError, prefix=pathT):
                countError += 1
        elif (file_full_name == True):
            pathT = dateT + "/" + args.FILE
            if (bucketError.object_exists(pathT) == True):
                countError += 1
    return countError


def exCount():
    # -d无效， 统计个数
    countAudio = 0
    countDecode = 0
    for dateT in dateList:
        if (all_files == True):
            pathT = dateT + "/"
            for obj in oss2.ObjectIterator(bucketAudio, prefix=pathT):
                if (obj.key == pathT):
                    continue
                countAudio += 1
            for obj in oss2.ObjectIterator(bucketDecode, prefix=pathT):
                if (obj.key == pathT):
                    continue
                countDecode += 1
        elif (access_id == True):
            pathT = dateT + "/" + args.FILE + "_"
            for obj in oss2.ObjectIterator(bucketAudio, prefix=pathT):
                countAudio += 1
            for obj in oss2.ObjectIterator(bucketDecode, prefix=pathT):
                countDecode += 1
        elif (file_full_name == True):
            pathT = dateT + "/" + args.FILE
            if (bucketAudio.object_exists(pathT) == True):
                countAudio += 1
            if (bucketDecode.object_exists(pathT) == True):
                countDecode += 1
    return (countAudio, countDecode)


def metaData(tmpFile):
    # 获取meta-data信息
    try:
        result = bucketDecode.head_object(tmpFile)
        result = result.headers
        return json.dumps(dict([("file-name", tmpFile), ("meta-data", dict(result))]), indent=4)
    except oss2.exceptions as e:
        return json.dumps(dict[("Exception", e.message)])


def metaDataErr(tmpFile):
    # 获取Err的meta-data信息
    try:
        result = bucketError.head_object(tmpFile)
        result = result.headers
        return json.dumps(dict([("file-name", tmpFile), ("meta-data", dict(result))]), indent=4)
    except oss2.exceptions as e:
        return json.dumps(dict[("Exception", e.message)])


def printErr():
    # 打印错误音频信息
    countError = 0
    print(error + " :")
    for dateT in dateList:
        # 打印error信息
        if (all_files == True):
            pathT = dateT + "/"
            for obj in oss2.ObjectIterator(bucketError, prefix=pathT):
                if (obj.key == pathT):
                    continue
                countError += 1
                print(metaDataErr(obj.key))
        elif (access_id == True):
            pathT = dateT + "/" + args.FILE + "_"
            for obj in oss2.ObjectIterator(bucketError, prefix=pathT):
                countError += 1
                print(metaDataErr(obj.key))
        elif (file_full_name == True):
            pathT = dateT + "/" + args.FILE
            if (bucketError.object_exists(pathT) == True):
                countError += 1
                print(metaDataErr(pathT))
    if (countError == 0):
        print("No Files Matching")
    return None


def exPrint():
    # 直接打印到屏幕
    countAudio = 0
    countDecode = 0
    print(audio + " :")
    for dateT in dateList:
        # 打印audio信息
        if (all_files == True):
            pathT = dateT + "/"
            for obj in oss2.ObjectIterator(bucketAudio, prefix=pathT):
                if (obj.key == pathT):
                    continue
                countAudio += 1
                print(metaData(obj.key))
        elif (access_id == True):
            pathT = dateT + "/" + args.FILE + "_"
            for obj in oss2.ObjectIterator(bucketAudio, prefix=pathT):
                countAudio += 1
                print(metaData(obj.key))
        elif (file_full_name == True):
            pathT = dateT + "/" + args.FILE
            if (bucketAudio.object_exists(pathT) == True):
                countAudio += 1
                print(metaData(pathT))
    if (countAudio == 0):
        print("No Files Matching")
    print("\n" + decode + " :")
    for dateT in dateList:
        # 打印decode信息
        if (all_files == True):
            pathT = dateT + "/"
            for obj in oss2.ObjectIterator(bucketDecode, prefix=pathT):
                if (obj.key == pathT):
                    continue
                countDecode += 1
                print(metaData(obj.key))
        elif (access_id == True):
            pathT = dateT + "/" + args.FILE + "_"
            for obj in oss2.ObjectIterator(bucketDecode, prefix=pathT):
                countDecode += 1
                print(metaData(obj.key))
        elif (file_full_name == True):
            pathT = dateT + "/" + args.FILE
            if (bucketDecode.object_exists(pathT) == True):
                countDecode += 1
                print(metaData(pathT))
    if (countDecode == 0):
        print("No Files Matching")
    return None


def chdir(pathD):
    # 改变工作目录
    if (os.path.isdir(pathD) == False):
        os.makedirs(pathD)
    os.chdir(pathD)
    return None


def downloadPcm(url, path):
    # 下载audio
    fileName = path + ".pcm"
    bucketAudio.get_object_to_file(url, fileName)
    return None


def downloadJson(url, path):
    # 下载decode
    fileName = path + ".json"
    bucketDecode.get_object_to_file(url, fileName)
    return None


def downloadErr(url, path):
    # 下载error
    fileName = path + ".json"
    bucketError.get_object_to_file(url, fileName)
    return None


def showProgress(now, tot):
    num = int(now / tot * 100)
    sys.stdout.write('\r')
    sys.stdout.write("%s%% |%s" % (int(num), int(num) * '>'))
    sys.stdout.flush()
    return None


def exDownloadErr(pathR):
    # 下载error信息
    now = 0
    countError = countErr()
    for dateT in dateList:
        pathD = pathR + "/" + dateT
        chdir(pathD)
        if (all_files == True):
            pathT = dateT + "/"
            for obj in oss2.ObjectIterator(bucketError, prefix=pathT):
                if (obj.key == pathT):
                    continue
                downloadErr(obj.key, obj.key[12:])
                now += 1
                showProgress(now, countError)
        elif (access_id == True):
            pathT = dateT + "/" + args.FILE + "_"
            for obj in oss2.ObjectIterator(bucketError, prefix=pathT):
                downloadErr(obj.key, obi.key[12:])
                now += 1
                showProgress(now, countError)
        elif (file_full_name == True):
            pathT = dateT + "/" + args.FILE
            if (bucketDecode.object_exists(pathT) == True):
                downloadErr(obj.key, args.FILE)
                now += 1
                showProgress(now, countError)
    sys.stdout.write('\n')
    return None


def exDownload(pathR):
    # 下载文件
    countAudio, countDecode = exCount()
    now = 0
    tot = countAudio + countDecode
    for dateT in dateList:
        pathD = pathR + "/" + dateT
        chdir(pathD)
        if (all_files == True):
            pathT = dateT + "/"
            for obj in oss2.ObjectIterator(bucketAudio, prefix=pathT):
                if (obj.key == pathT):
                    continue
                downloadPcm(obj.key, obj.key[12:])
                now += 1
                showProgress(now, tot)
            for obj in oss2.ObjectIterator(bucketDecode, prefix=pathT):
                if (obj.key == pathT):
                    continue
                downloadJson(obj.key, obj.key[12:])
                now += 1
                showProgress(now, tot)
        elif (access_id == True):
            pathT = dateT + "/" + args.FILE + "_"
            for obj in oss2.ObjectIterator(bucketAudio, prefix=pathT):
                downloadPcm(obj.key, obj.key[12:])
                now += 1
                showProgress(now, tot)
            for obj in oss2.ObjectIterator(bucketDecode, prefix=pathT):
                downloadJson(obj.key, obi.key[12:])
                now += 1
                showProgress(now, tot)
        elif (file_full_name == True):
            pathT = dateT + "/" + args.FILE
            if (bucketAudio.object_exists(pathT) == True):
                downloadPcm(obj.key, args.FILE)
                now += 1
                showProgress(now, tot)
            if (bucketDecode.object_exists(pathT) == True):
                downloadJson(obj.key, args.FILE)
                now += 1
                showProgress(now, tot)
    sys.stdout.write('\n')
    return None


def main():
    setParse()
    setBucket()
    if (prepare() == False):
        pass
    elif (args.count == True):
        if (args.error == True):
            countError = countErr()
            print(error + " : %d" % (countError))
        else:
            countAudio, countDecode = exCount()
            print(audio + " : %d" % (countAudio))
            print(decode + " : %d" % (countDecode))
    elif args.download == False:
        if (args.error == True):
            printErr()
        else:
            exPrint()
    else:
        pathT = args.PATH
        if (os.path.isdir(pathT) == False):
            print("Invalid Path")
        elif (os.listdir(pathT)):
            pathT = os.path.abspath(pathT) + "/oss_dump"
            if (os.path.isdir(pathT) == False):
                os.makedirs(pathT)
        if (args.error == True):
            exDownloadErr(pathT)
        else:
            exDownload(pathT)


if __name__ == "__main__":
    main()
