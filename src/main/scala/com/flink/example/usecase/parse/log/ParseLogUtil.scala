package com.flink.example.usecase.parse.log

import java.text.SimpleDateFormat

object ParseLogUtil {
  def parseHDFSAuditLog(log : String) = {
    var index0 = log.indexOf(" ")
    index0 = log.indexOf(" ", index0 + 1)
    val data = log.substring(0, index0).trim
    val index1 = log.indexOf("allowed=")
    val len1 = 8
    val index2 = log.indexOf("ugi=")
    val len2 = 4
    val index3 = log.indexOf("ip=/")
    val len3 = 4
    val index4 = log.indexOf("cmd=")
    val len4 = 4
    val index5 = log.indexOf("src=")
    val len5 = 4
    val index6 = log.indexOf("dst=")
    val len6 = 4
    val index7 = log.indexOf("perm=")

    val timeStampMS = getTimeStampMS(matchTime(log))
    val allowed = log.substring(index1 + len1, index2).trim
    val ugi = log.substring(index2 + len2, index3).trim
    val ip = log.substring(index3 + len3, index4).trim
    val cmd = log.substring(index4 + len4, index5).trim
    var src = log.substring(index5 + len5, index6).trim
    var dst = log.substring(index6 + len6, index7).trim

    (timeStampMS, allowed, ugi, ip, cmd, src, dst)
  }

  def matchTime(log : String) = {
    import java.util.regex.Pattern
    val pattern = Pattern.compile("[0-9]{4}[-][0-9]{2}[-][0-9]{2}[ ][0-9]{2}[:][0-9]{2}[:][0-9]{2},[0-9]{3}")
    val matcher = pattern.matcher(log)
    if (matcher.find()) {
      matcher.group()
    } else {
      "-1"
    }
  }

  def getTimeStampMS(timeStr : String) = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS")
    df.parse(timeStr).getTime
  }

}
