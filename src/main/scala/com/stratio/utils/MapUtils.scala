package com.stratio.utils

object MapUtils {

  def updateMap[T](mapToUpdate: Map[T, Int], key: T): Map[T, Int] =
    mapToUpdate.updated(key, mapToUpdate.getOrElse(key, 0) + 1)

  def updateMapWithOption[T](mapToUpdate: Map[T, Int], key: Option[T]): Map[T, Int] = key match{
    case Some(keyValue) => mapToUpdate.updated(keyValue, mapToUpdate.getOrElse(keyValue, 0) + 1)
    case None => mapToUpdate
  }

  def aggregateMaps[T](map1: Map[T, Int], map2: Map[T, Int]): Map[T, Int] =
    map1 ++ map2.map(keyValue => (keyValue._1, keyValue._2 + map1.getOrElse(keyValue._1, 0)))

}
