
/**
  * Created by eecheverri on 3/16/19.
  */

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import java.io.{File, IOException}
import java.sql.Date
import scala.collection.mutable._

import com.typesafe.config.{Config, ConfigFactory}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, functions}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{expr, sum, udf, _}
import org.slf4j.LoggerFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.Column

import scala.collection.mutable
import scala.util.{Failure, Success}
import org.apache.spark.scheduler._
import org.apache.log4j.LogManager

import scala.collection.mutable.ListBuffer


object ReportEpisodeSummary extends App{



  // case classes
  //TODO define as a POSO  Scala classes??  difference between case class and class ?

  case class ReportEpisodeSummary( Filter_ID: Integer,Episode_ID: String, Episode_Name: String, Episode_Description: String, Episode_Type: String, MDC: Integer, MDC_Description: String, Level: Integer, Episode_Volume: Integer, Split_Total_Cost: Double, Split_Average_Cost: Double, Split_Min_Cost: Double, Split_Max_Cost: Double, Split_STDEV: Double, Split_CV: Double, Split_Total_PAC_Cost: Double, Split_Average_PAC_Cost: Double, Split_PAC_Percent: Double, Split_Total_Typical_Cost: Double, Split_Average_Typical_Cost: Double, Split_Total_TypicalwPAC_Cost: Double, Split_Average_TypicalwPAC_Cost: Double)
  case class Episode(EPISODE_ID: String, NAME: String, TYPE: String, STATUS: String, DESCRIPTION: String, CREATED_DATE: Date, MODIFIED_DATE: Date, USERS_USER_ID: String, MDC_CATEGORY: String, PARM_SET: Integer, TRIGGER_TYPE: Integer, TRIGGER_NUMBER: Integer, SEPARATION_MIN: Integer, SEPARATION_MAX: Integer, BOUND_OFFSET: Integer, BOUND_LENGTH: Integer, CONDITION_MIN: Integer, VERSION: String, END_OF_STUDY: Integer)
  case class Mel (id: Integer, filter_id: Integer, master_episode_id: String, claim_type: String, level: Integer, split: Integer, annualized: Integer, cost: Double, cost_t: Double, cost_tc: Double, cost_c: Double, risk_factor_count: Integer, sub_type_count: Integer, probability_of_complications: Integer, IP_stay_count: Integer, IP_stay_facility_costs: Double, IP_stay_prof_costs: Double, IP_stay_total_costs: Double, IP_stay_bed_days: Integer, IP_stay_avg_length: Integer)
  case class mdcDesc(mdc: String, mdc_description: String)
  case class subDistinct(child_master_episode_id: String, association_level: Integer)
  case class subDistinctFilter(child_master_episode_id: String, association_level: Integer)
  case class percentiles(Filter_ID: Integer, Episode_ID: String, Level: Integer, Split_1stPercentile_Cost: Double, Split_99thPercentile_Cost: Double, Split_80thPercentile_Cost: Double, Annualized_Split_1stPercentile_Cost: Double, Annualized_Split_99thPercentile_Cost: Double, Annualized_Split_80thPercentile_Cost: Double, Unsplit_1stPercentile_Cost: Double, Unsplit_99thPercentile_Cost: Double, Annualized_Unsplit_1stPercentile_Cost: Double, Annualized_Unsplit_99thPercentile_Cost: Double)
  // case class ra_exp_cost(row_names: String, epi_number : String, epi_name : String, epi_id : String, eol_prob : Double, use_prob_ra_typ_ip_l1 : Double, cost_pred_ra_typ_ip_l1 : Double, exp_cost_ra_typ_ip_l1 : Double, use_prob_sa_typ_ip_l1 : Double, cost_pred_sa_typ_ip_l1 : Double, exp_cost_sa_typ_ip_l1 : Double, use_prob_ra_typ_other_l1 : Double, cost_pred_ra_typ_other_l1 : Double, exp_cost_ra_typ_other_l1 : Double, use_prob_sa_typ_other_l1 : Double, cost_pred_sa_typ_other_l1 : Double, exp_cost_sa_typ_other_l1 : Double, use_prob_ra_comp_other_l1 : Double, cost_pred_ra_comp_other_l1 : Double, exp_cost_ra_comp_other_l1 : Double, use_prob_sa_comp_other_l1 : Double, cost_pred_sa_comp_other_l1 : Double, exp_cost_sa_comp_other_l1 : Double, total_exp_cost_ra_l1 : Double, total_exp_cost_sa_l1 : Double, use_prob_ra_typ_l1 : Double, cost_pred_ra_typ_l1 : Double, exp_cost_ra_typ_l1 : Double, use_prob_sa_typ_l1 : Double, cost_pred_sa_typ_l1 : Double, exp_cost_sa_typ_l1 : Double, use_prob_ra_comp_l1 : Double, cost_pred_ra_comp_l1 : Double, exp_cost_ra_comp_l1 : Double, use_prob_sa_comp_l1 : Double, cost_pred_sa_comp_l1 : Double, exp_cost_sa_comp_l1 : Double, use_prob_ra_typ_ip_l3 : Double, cost_pred_ra_typ_ip_l3 : Double, exp_cost_ra_typ_ip_l3 : Double, use_prob_sa_typ_ip_l3 : Double, cost_pred_sa_typ_ip_l3 : Double, exp_cost_sa_typ_ip_l3 : Double, use_prob_ra_typ_other_l3 : Double, cost_pred_ra_typ_other_l3 : Double, exp_cost_ra_typ_other_l3 : Double, use_prob_sa_typ_other_l3 : Double, cost_pred_sa_typ_other_l3 : Double, exp_cost_sa_typ_other_l3 : Double, use_prob_ra_comp_other_l3 : Double, cost_pred_ra_comp_other_l3 : Double, exp_cost_ra_comp_other_l3 : Double, use_prob_sa_comp_other_l3 : Double, cost_pred_sa_comp_other_l3 : Double, exp_cost_sa_comp_other_l3 : Double, total_exp_cost_ra_l3 : Double, total_exp_cost_sa_l3 : Double, use_prob_ra_typ_ip_l4 : Double, cost_pred_ra_typ_ip_l4 : Double, exp_cost_ra_typ_ip_l4 : Double, use_prob_sa_typ_ip_l4 : Double, cost_pred_sa_typ_ip_l4 : Double, exp_cost_sa_typ_ip_l4 : Double, use_prob_ra_typ_other_l4 : Double, cost_pred_ra_typ_other_l4 : Double, exp_cost_ra_typ_other_l4 : Double, use_prob_sa_typ_other_l4 : Double, cost_pred_sa_typ_other_l4 : Double, exp_cost_sa_typ_other_l4 : Double, use_prob_ra_comp_other_l4 : Double, cost_pred_ra_comp_other_l4 : Double, exp_cost_ra_comp_other_l4 : Double, use_prob_sa_comp_other_l4 : Double, cost_pred_sa_comp_other_l4 : Double, exp_cost_sa_comp_other_l4 : Double, total_exp_cost_ra_l4 : Double, total_exp_cost_sa_l4 : Double, use_prob_ra_typ_l5 : Double, cost_pred_ra_typ_l5 : Double, exp_cost_ra_typ_l5 : Double, use_prob_sa_typ_l5 : Double, cost_pred_sa_typ_l5 : Double, exp_cost_sa_typ_l5 : Double, use_prob_ra_comp_l5 : Double, cost_pred_ra_comp_l5 : Double, exp_cost_ra_comp_l5 : Double, use_prob_sa_comp_l5 : Double, cost_pred_sa_comp_l5 : Double, exp_cost_sa_comp_l5 : Double, total_exp_cost_ra_l5 : Double, total_exp_cost_sa_l5 : Double)
  case class ra_exp_cost(row_names: String,epi_number: String,epi_name: String,epi_id: String,eol_prob: Double,use_prob_ra_typ_ip_l1: Double,cost_pred_ra_typ_ip_l1: Double,exp_cost_ra_typ_ip_l1: Double,use_prob_sa_typ_ip_l1: Double,cost_pred_sa_typ_ip_l1: Double,exp_cost_sa_typ_ip_l1: Double,use_prob_ra_typ_other_l1: Double,cost_pred_ra_typ_other_l1: Double,exp_cost_ra_typ_other_l1: Double,use_prob_sa_typ_other_l1: Double,cost_pred_sa_typ_other_l1: Double,exp_cost_sa_typ_other_l1: Double,use_prob_ra_comp_other_l1: Double,cost_pred_ra_comp_other_l1: Double,exp_cost_ra_comp_other_l1: Double,use_prob_sa_comp_other_l1: Double,cost_pred_sa_comp_other_l1: Double,exp_cost_sa_comp_other_l1: Double,use_prob_ra_typ_ip_l4: Double,cost_pred_ra_typ_ip_l4: Double,exp_cost_ra_typ_ip_l4: Double,use_prob_sa_typ_ip_l4: Double,cost_pred_sa_typ_ip_l4: Double,exp_cost_sa_typ_ip_l4: Double,use_prob_ra_typ_other_l4: Double,cost_pred_ra_typ_other_l4: Double,exp_cost_ra_typ_other_l4: Double,use_prob_sa_typ_other_l4: Double,cost_pred_sa_typ_other_l4: Double,exp_cost_sa_typ_other_l4: Double,use_prob_ra_comp_other_l4: Double,cost_pred_ra_comp_other_l4: Double,exp_cost_ra_comp_other_l4: Double,use_prob_sa_comp_other_l4: Double,cost_pred_sa_comp_other_l4: Double,exp_cost_sa_comp_other_l4: Double,total_exp_cost_ra_l1: Double,total_exp_cost_sa_l1: Double,total_exp_cost_ra_l4: Double,total_exp_cost_sa_l4: Double,use_prob_ra_typ_l1: Double,cost_pred_ra_typ_l1: Double,exp_cost_ra_typ_l1: Double,use_prob_sa_typ_l1: Double,cost_pred_sa_typ_l1: Double,exp_cost_sa_typ_l1: Double,use_prob_ra_comp_l1: Double,cost_pred_ra_comp_l1: Double,exp_cost_ra_comp_l1: Double,use_prob_sa_comp_l1: Double,cost_pred_sa_comp_l1: Double,exp_cost_sa_comp_l1: Double,use_prob_ra_typ_l5: Double,cost_pred_ra_typ_l5: Double,exp_cost_ra_typ_l5: Double,use_prob_sa_typ_l5: Double,cost_pred_sa_typ_l5: Double,exp_cost_sa_typ_l5: Double,use_prob_ra_comp_l5: Double,cost_pred_ra_comp_l5: Double,exp_cost_ra_comp_l5: Double,use_prob_sa_comp_l5: Double,cost_pred_sa_comp_l5: Double,exp_cost_sa_comp_l5: Double,total_exp_cost_ra_l5: Double,total_exp_cost_sa_l5: Double,use_prob_ra_typ_other_l3: Double,cost_pred_ra_typ_other_l3: Double,exp_cost_ra_typ_other_l3: Double,use_prob_sa_typ_other_l3: Double,cost_pred_sa_typ_other_l3: Double,exp_cost_sa_typ_other_l3: Double,use_prob_ra_comp_other_l3: Double,cost_pred_ra_comp_other_l3: Double,exp_cost_ra_comp_other_l3: Double,use_prob_sa_comp_other_l3: Double,cost_pred_sa_comp_other_l3: Double,exp_cost_sa_comp_other_l3: Double,total_exp_cost_ra_l3: Double,total_exp_cost_sa_l3: Double,use_prob_ra_typ_ip_l3: Double,cost_pred_ra_typ_ip_l3: Double,exp_cost_ra_typ_ip_l3: Double,use_prob_sa_typ_ip_l3: Double,cost_pred_sa_typ_ip_l3: Double,exp_cost_sa_typ_ip_l3: Double )



  override def main(args: Array[String]): Unit = {


    // Instantiate classes
    val appConf = new AppConfiguration()
    val appConnection = new ScalaJdbcConnect()
    var sQLStrings = new SQLStrings()

    // Encoders
    val episodeschema: StructType = Encoders.product[Episode].schema
    val melschema: StructType = Encoders.product[Mel].schema
    val mdcdescschema: StructType = Encoders.product[mdcDesc].schema
    val subdistinctschema: StructType = Encoders.product[subDistinct].schema
    val subdistinctfilterschema: StructType = Encoders.product[subDistinctFilter].schema
    val percentileschema: StructType = Encoders.product[percentiles].schema
    val raexpcostschema: StructType = Encoders.product[ra_exp_cost].schema




    //TODO loop over bounds
    var lowerbound : Int  = 1
    var upperbound : Int = 10000
    var numpartitions : Int = 1



    val JOBUID : String  = args(1).toString





    val sparkSession = SparkSession.
      builder().
      appName("PROM ReportEpisodeDetail Scala").
      enableHiveSupport().
      getOrCreate()


    val url_ecr_connection_String : String = sQLStrings.ecr_jobName.concat(JOBUID)
    val schema_name = appConnection.getSQLSchema(url_ecr_connection_String)
    val url_schema_connection_string : String = appConf.envOrElseConfig("prd.url").concat(schema_name)
    val episode_builder_url  = appConf.envOrElseConfig("prd.episode_builder_url")




    val dfEpisodes = appConnection.getBoundedDataFrame("SELECT * FROM episode",sparkSession,url_schema_connection_string)
    val DFassignment = appConnection.getBoundedDataFrame("SELECT * FROM assignment",sparkSession,url_schema_connection_string)
    val dfEpisode = appConnection.getBoundedDataFrame("SELECT * FROM episode",sparkSession,episode_builder_url)
    val dfassociation = appConnection.getBoundedDataFrame("SELECT * FROM association",sparkSession,url_schema_connection_string)
    val dfMel = appConnection.getBoundedDataFrame("SELECT * FROM master_epid_level",sparkSession,url_schema_connection_string)
    val dfMdc = appConnection.getBoundedDataFrame("SELECT * FROM mdc_desc",sparkSession,url_schema_connection_string)
    val dfraexpcost = appConnection.getBoundedDataFrame("SELECT * FROM ra_exp_cost ",sparkSession,url_schema_connection_string)
    val dffiltered_episodes = appConnection.getBoundedDataFrame("SELECT * FROM filtered2",sparkSession,url_schema_connection_string)

    dfMel.show()


  }



  def getReportEpisodeSummaryDataFrame(dfMelbyEpisode : DataFrame, dfEpisode: DataFrame, dfMdc: DataFrame, dfsub: DataFrame, dfsubfilter: DataFrame): DataFrame = {

    // Joins
    val rf1:DataFrame = dfMelbyEpisode.join(dfEpisode, Seq("episode_id"), "inner")
      .join(dfMdc, dfMdc("mdc") === dfEpisode("MDC_CATEGORY"), "left")
      .join(dfsub, dfMelbyEpisode("master_episode_id") === dfsub("child_master_episode_id"), "left")
      .join(dfsubfilter, dfMelbyEpisode("master_episode_id") === dfsubfilter("child_master_episode_id"), "left")
      .filter(dfMelbyEpisode("level") < dfsub("association_level")
        && dfMelbyEpisode("claim_type").equalTo("CL")
        && dfMelbyEpisode("split") === 1
        && dfMelbyEpisode("annualized") === 0
        && dfMelbyEpisode("filter_id") === 0
        && dfMelbyEpisode("level").isin(1, 2, 3, 4, 5)
        || dfsub("association_level").isNull
        && dfMelbyEpisode("claim_type").equalTo("CL")
        && dfMelbyEpisode("split") === 1
        && dfMelbyEpisode("annualized") === 0
        && dfMelbyEpisode("filter_id") === 0
        && dfMelbyEpisode("level").isin(1, 2, 3, 4, 5)
        || dfMelbyEpisode("level") < dfsubfilter("association_level")
        && dfMelbyEpisode("claim_type").equalTo("CL")
        && dfMelbyEpisode("split") === 1
        && dfMelbyEpisode("annualized") === 0
        && dfMelbyEpisode("filter_id") === 1
        && dfMelbyEpisode("level").isin(1, 2, 3, 4, 5)
        || dfsubfilter("association_level").isNull
        && dfMelbyEpisode("claim_type").equalTo("CL")
        && dfMelbyEpisode("split") === 1
        && dfMelbyEpisode("annualized") === 0
        && dfMelbyEpisode("filter_id") === 1
        && dfMelbyEpisode("level").isin(1, 2, 3, 4, 5)

      )
      //  .select("episode_id", "filter_id", "NAME", "DESCRIPTION", "level", "TYPE", "MDC_CATEGORY", "cost", "cost_c", "cost_t", "cost_tc", "master_episode_id")
      .groupBy("episode_id", "level", "filter_id","NAME", "DESCRIPTION","TYPE", "MDC_CATEGORY")
      .agg(
        countDistinct("master_episode_id").as("Episode_Volume"),
        sum("cost").as("Split_Total_Cost"),
        (sum("cost") / countDistinct("master_episode_id")).as("Split_Average_Cost"),
        min("cost").as("Split_Min_Cost"),
        max("cost").as("Split_Max_Cost"),
        stddev_pop("cost").as("Split_STDEV"),
        (stddev_pop("cost") / (sum("cost") / count("master_episode_id"))).as("Split_CV"),
        sum("cost_c").as("Split_Total_PAC_Cost"),
        (sum("cost_c") / count("master_episode_id")).as("Split_Average_PAC_Cost"),
        ((sum("cost_c") / sum("cost")) * 100).as("Split_PAC_Percent"),
        sum("cost_t").as("Split_Total_Typical_Cost"),
        (sum("cost_t") / count("master_episode_id")).as("Split_Average_Typical_Cost"),
        sum("cost_tc").as("Split_Total_TypicalwPAC_Cost"),
        (sum("cost_tc") / count("master_episode_id")).as("Split_Average_TypicalwPAC_Cost")
      )

    return rf1


  }


  def getDataFrameCost(dfMelbyEpisode: DataFrame, Sequence: Int, claimType: String, split: Integer, annualized: Int): DataFrame = {

    // 0 -13 -> 14 columns

    val melSumString = Array(
      Array("Unsplit_Episode_Volume", "Unsplit_Total_Cost", "Unsplit_Average_Cost", "Unsplit_Min_Cost", "Unsplit_Max_Cost", "Unsplit_STDEV", "Unsplit_CV", "Unsplit_Total_PAC_Cost", "Unsplit_Average_PAC_Cost", "Unsplit_PAC_Percent", "Unsplit_Total_Typical_Cost","Unsplit_Average_Typical_Cost", "Unsplit_Total_TypicalwPAC_Cost", "Unsplit_Average_TypicalwPAC_Cost"),
      Array("Annualized_Split_Episode_Volume", "Annualized_Split_Total_Cost", "Annualized_Split_Average_Cost", "Annualized_Split_Min_Cost", "Annualized_Split_Max_Cost", "Annualized_Split_STDEV", "Annualized_Split_CV", "Annualized_Split_Total_PAC_Cost", "Annualized_Split_Average_PAC_Cost", "Annualized_Split_PAC_Percent", "Annualized_Split_Total_Typical_Cost", "Annualized_Split_Average_Typical_Cost", "Annualized_Split_Total_TypicalwPAC_Cost", "Annualized_Split_Average_TypicalwPAC_Cost"),
      Array("Annualized_Unsplit_Episode_Volume", "Annualized_Unsplit_Total_Cost", "Annualized_Unsplit_Average_Cost", "Annualized_Unsplit_Min_Cost", "Annualized_Unsplit_Max_Cost", "Annualized_Unsplit_STDEV", "Annualized_Unsplit_CV", "Annualized_Unsplit_Total_PAC_Cost", "Annualized_Unsplit_Average_PAC_Cost", "Annualized_Unsplit_PAC_Percent", "Annualized_Unsplit_Total_Typical_Cost", "Annualized_Unsplit_Average_Typical_Cost", "Annualized_Unsplit_Total_TypicalwPAC_Cost", "Annualized_Unsplit_Average_TypicalwPAC_Cost")
    )

    val melSum1 = dfMelbyEpisode
      .filter(
        dfMelbyEpisode("claim_type").equalTo(claimType)
          && dfMelbyEpisode("split").equalTo(split)
          && dfMelbyEpisode("annualized").equalTo(annualized)
      )
      .select("episode_id", "filter_id", "level", "cost", "cost_c", "cost_t", "cost_tc","master_episode_id","claim_type","split","annualized")
      .groupBy("episode_id", "level", "filter_id")
      .agg(
        countDistinct("master_episode_id").as(melSumString(Sequence)(0)),
        sum("cost").as(melSumString(Sequence)(1)),
        (sum("cost") / countDistinct("master_episode_id")).as(melSumString(Sequence)(2)),
        min("cost").as(melSumString(Sequence)(3)),
        max("cost").as(melSumString(Sequence)(4)),
        stddev_pop("cost").as(melSumString(Sequence)(5)),
        (stddev_pop("cost") / (sum("cost") / countDistinct("master_episode_id"))).as(melSumString(Sequence)(6)),
        sum("cost_c").as(melSumString(Sequence)(7)),
        (sum("cost_c") / countDistinct("master_episode_id")).as(melSumString(Sequence)(8)),
        ((sum("cost_c") / sum("cost")) * 100).as(melSumString(Sequence)(9)),
        sum("cost_t").as(melSumString(Sequence)(10)),
        (sum("cost_t") / countDistinct("master_episode_id")).as(melSumString(Sequence)(11)),
        sum("cost_tc").as(melSumString(Sequence)(12)),
        (sum("cost_tc") / countDistinct("master_episode_id")).as(melSumString(Sequence)(13))
      )

    return melSum1

  }


  def getRaExpCostDataFrame( dfMelbyEpisode: DataFrame, dfraexpcost: DataFrame, Sequence: Int, level: Integer, query: Int): DataFrame = {

    val dataFrameCostArrayBuffer = mutable.ArrayBuffer[DataFrame]()

    val column_name_0 = Array(Array("Episode_Other_Count","Expected_Split_Average_Cost", "Expected_Split_Typical_Other_Average_Cost", "Expected_Split_PAC_Average_Cost", "Expected_Unsplit_Average_Cost", "Expected_Unsplit_Typical_Other_Average_Cost", "Expected_Unsplit_PAC_Average_Cost"))
    val column_name_1 = Array(Array("Episode_IP_Count", "Expected_Split_Average_Cost", "Expected_Split_Typical_IP_Average_Cost", "Expected_Split_Typical_Other_Average_Cost", "Expected_Split_PAC_Average_Cost", "Expected_Unsplit_Average_Cost", "Expected_Unsplit_Typical_IP_Average_Cost", "Expected_Unsplit_Typical_Other_Average_Cost", "Expected_Unsplit_PAC_Average_Cost"))

    val raExpCostStrings_0 = Array(
      Array("exp_cost_sa_typ_l1", "episode_id", "total_exp_cost_sa_l1", "exp_cost_sa_typ_l1", "exp_cost_sa_comp_l1", "total_exp_cost_ra_l1", "exp_cost_ra_typ_l1", "exp_cost_ra_comp_l1"),
      Array("exp_cost_sa_typ_l5", "episode_id", "total_exp_cost_sa_l5", "exp_cost_sa_typ_l5", "exp_cost_sa_comp_l5", "total_exp_cost_ra_l5", "exp_cost_ra_typ_l5", "exp_cost_ra_comp_l5"
      )
    )

    val raExpCostStrings_1 = Array(
      Array("exp_cost_sa_typ_ip_l1", "episode_id", "exp_cost_sa_typ_ip_l1", "total_exp_cost_sa_l1", "exp_cost_sa_typ_other_l1", "exp_cost_sa_comp_other_l1", "total_exp_cost_ra_l1", "exp_cost_ra_typ_ip_l1", "exp_cost_ra_typ_other_l1", "exp_cost_ra_comp_other_l1"),
      Array("exp_cost_sa_typ_ip_l3", "episode_id", "total_exp_cost_sa_l3", "exp_cost_sa_typ_ip_l3", "exp_cost_sa_typ_other_l3", "exp_cost_sa_comp_other_l3", "total_exp_cost_ra_l3", "exp_cost_ra_typ_ip_l3", "exp_cost_ra_typ_other_l3", "exp_cost_ra_comp_other_l3"),
      Array("exp_cost_sa_typ_ip_l4", "episode_id", "total_exp_cost_sa_l4", "exp_cost_sa_typ_ip_l4", "exp_cost_sa_typ_other_l4", "exp_cost_sa_comp_other_l4", "total_exp_cost_ra_l4", "exp_cost_ra_typ_ip_l4", "exp_cost_ra_typ_other_l4", "exp_cost_ra_comp_other_l4")
    )


    val Query = query match {


      case 0 =>

        dataFrameCostArrayBuffer += dfraexpcost.join(dfMelbyEpisode, dfMelbyEpisode("episode_id") === dfraexpcost("epi_number"), "left")
          .filter(
            dfraexpcost(raExpCostStrings_0(Sequence)(0)).isNotNull
              && dfMelbyEpisode("level").equalTo(level)
          )
          .select("episode_id", "level", "filter_id","master_episode_id",
            raExpCostStrings_0(Sequence)(0),raExpCostStrings_0(Sequence)(2), raExpCostStrings_0(Sequence)(3), raExpCostStrings_0(Sequence)(4), raExpCostStrings_0(Sequence)(5), raExpCostStrings_0(Sequence)(6), raExpCostStrings_0(Sequence)(7)
          )
          .groupBy("episode_id", "level", "filter_id")
          .agg(
            countDistinct("master_episode_id").as(column_name_0(0)(0)),
            ((sum(raExpCostStrings_0(Sequence)(2)) / countDistinct("master_episode_id"))).as(column_name_0(0)(1)),
            ((sum(raExpCostStrings_0(Sequence)(3)) / countDistinct("master_episode_id"))).as(column_name_0(0)(2)),
            ((sum(raExpCostStrings_0(Sequence)(4)) / countDistinct("master_episode_id"))).as(column_name_0(0)(3)),
            ((sum(raExpCostStrings_0(Sequence)(5)) / countDistinct("master_episode_id"))).as(column_name_0(0)(4)),
            ((sum(raExpCostStrings_0(Sequence)(6)) / countDistinct("master_episode_id"))).as(column_name_0(0)(5)),
            ((sum(raExpCostStrings_0(Sequence)(7)) / countDistinct("master_episode_id"))).as(column_name_0(0)(6))
          )


      case 1 =>

        dataFrameCostArrayBuffer +=  dfraexpcost.join(dfMelbyEpisode, dfMelbyEpisode("episode_id") === dfraexpcost("epi_number"), "left")
          .filter(
            dfraexpcost(raExpCostStrings_1(Sequence)(0)).isNotNull
              && dfMelbyEpisode("level").equalTo(level)
          ).select("episode_id", "level", "filter_id","master_episode_id",
          raExpCostStrings_1(Sequence)(0),raExpCostStrings_1(Sequence)(2), raExpCostStrings_1(Sequence)(3), raExpCostStrings_1(Sequence)(4), raExpCostStrings_1(Sequence)(5), raExpCostStrings_1(Sequence)(6), raExpCostStrings_1(Sequence)(7), raExpCostStrings_1(Sequence)(8),raExpCostStrings_1(Sequence)(9)
        )
          .groupBy("episode_id", "level", "filter_id")
          .agg(
            countDistinct("master_episode_id").as(column_name_1(0)(0)),
            (sum(raExpCostStrings_1(Sequence)(2)) / countDistinct("master_episode_id")).as(column_name_1(0)(1)),
            (sum(raExpCostStrings_1(Sequence)(3)) / countDistinct("master_episode_id")).as(column_name_1(0)(2)),
            (sum(raExpCostStrings_1(Sequence)(4)) / countDistinct("master_episode_id")).as(column_name_1(0)(3)),
            (sum(raExpCostStrings_1(Sequence)(5)) / countDistinct("master_episode_id")).as(column_name_1(0)(4)),
            (sum(raExpCostStrings_1(Sequence)(6)) / countDistinct("master_episode_id")).as(column_name_1(0)(5)),
            (sum(raExpCostStrings_1(Sequence)(7)) / countDistinct("master_episode_id")).as(column_name_1(0)(6)),
            (sum(raExpCostStrings_1(Sequence)(8)) / countDistinct("master_episode_id")).as(column_name_1(0)(7)),
            (sum(raExpCostStrings_1(Sequence)(9)) / countDistinct("master_episode_id")).as(column_name_1(0)(8))
          )


    }
    return dataFrameCostArrayBuffer.last
  }







}