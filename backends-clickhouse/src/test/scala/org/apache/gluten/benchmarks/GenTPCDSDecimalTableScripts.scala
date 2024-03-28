/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.benchmarks

import scala.collection.mutable.ArrayBuffer

object GenTPCDSDecimalTableScripts {

  val catalogSalesFields =
    s"""
       |  cs_sold_date_sk          INT,
       |  cs_sold_time_sk          INT,
       |  cs_ship_date_sk          INT,
       |  cs_bill_customer_sk      INT,
       |  cs_bill_cdemo_sk         INT,
       |  cs_bill_hdemo_sk         INT,
       |  cs_bill_addr_sk          INT,
       |  cs_ship_customer_sk      INT,
       |  cs_ship_cdemo_sk         INT,
       |  cs_ship_hdemo_sk         INT,
       |  cs_ship_addr_sk          INT,
       |  cs_call_center_sk        INT,
       |  cs_catalog_page_sk       INT,
       |  cs_ship_mode_sk          INT,
       |  cs_warehouse_sk          INT,
       |  cs_item_sk               INT,
       |  cs_promo_sk              INT,
       |  cs_order_number          LONG,
       |  cs_quantity              INT,
       |  cs_wholesale_cost        decimal(7,2),
       |  cs_list_price            decimal(7,2),
       |  cs_sales_price           decimal(7,2),
       |  cs_ext_discount_amt      decimal(7,2),
       |  cs_ext_sales_price       decimal(7,2),
       |  cs_ext_wholesale_cost    decimal(7,2),
       |  cs_ext_list_price        decimal(7,2),
       |  cs_ext_tax               decimal(7,2),
       |  cs_coupon_amt            decimal(7,2),
       |  cs_ext_ship_cost         decimal(7,2),
       |  cs_net_paid              decimal(7,2),
       |  cs_net_paid_inc_tax      decimal(7,2),
       |  cs_net_paid_inc_ship     decimal(7,2),
       |  cs_net_paid_inc_ship_tax decimal(7,2),
       |  cs_net_profit            decimal(7,2)
       |""".stripMargin

  val catalogReturnsFields =
    s"""
       |  cr_returned_date_sk      INT,
       |  cr_returned_time_sk      INT,
       |  cr_item_sk               INT,
       |  cr_refunded_customer_sk  INT,
       |  cr_refunded_cdemo_sk     INT,
       |  cr_refunded_hdemo_sk     INT,
       |  cr_refunded_addr_sk      INT,
       |  cr_returning_customer_sk INT,
       |  cr_returning_cdemo_sk    INT,
       |  cr_returning_hdemo_sk    INT,
       |  cr_returning_addr_sk     INT,
       |  cr_call_center_sk        INT,
       |  cr_catalog_page_sk       INT,
       |  cr_ship_mode_sk          INT,
       |  cr_warehouse_sk          INT,
       |  cr_reason_sk             INT,
       |  cr_order_number          LONG,
       |  cr_return_quantity       INT,
       |  cr_return_amount         decimal(7,2),
       |  cr_return_tax            decimal(7,2),
       |  cr_return_amt_inc_tax    decimal(7,2),
       |  cr_fee                   decimal(7,2),
       |  cr_return_ship_cost      decimal(7,2),
       |  cr_refunded_cash         decimal(7,2),
       |  cr_reversed_charge       decimal(7,2),
       |  cr_store_credit          decimal(7,2),
       |  cr_net_loss              decimal(7,2)
       |""".stripMargin

  val inventoryFields =
    s"""
       |  inv_date_sk          INT,
       |  inv_item_sk          INT,
       |  inv_warehouse_sk     INT,
       |  inv_quantity_on_hand INT
       |""".stripMargin

  val storeSalesFields =
    s"""
       |  ss_sold_date_sk      INT,
       |  ss_sold_time_sk      INT,
       |  ss_item_sk           INT,
       |  ss_customer_sk       INT,
       |  ss_cdemo_sk          INT,
       |  ss_hdemo_sk          INT,
       |  ss_addr_sk           INT,
       |  ss_store_sk          INT,
       |  ss_promo_sk          INT,
       |  ss_ticket_number     LONG,
       |  ss_quantity          INT,
       |  ss_wholesale_cost     decimal(7,2),
       |  ss_list_price         decimal(7,2),
       |  ss_sales_price        decimal(7,2),
       |  ss_ext_discount_amt   decimal(7,2),
       |  ss_ext_sales_price    decimal(7,2),
       |  ss_ext_wholesale_cost decimal(7,2),
       |  ss_ext_list_price     decimal(7,2),
       |  ss_ext_tax            decimal(7,2),
       |  ss_coupon_amt         decimal(7,2),
       |  ss_net_paid           decimal(7,2),
       |  ss_net_paid_inc_tax   decimal(7,2),
       |  ss_net_profit         decimal(7,2)
       |""".stripMargin

  val storeReturnsFields =
    s"""
       |  sr_returned_date_sk  INT,
       |  sr_return_time_sk    INT,
       |  sr_item_sk           INT,
       |  sr_customer_sk       INT,
       |  sr_cdemo_sk          INT,
       |  sr_hdemo_sk          INT,
       |  sr_addr_sk           INT,
       |  sr_store_sk          INT,
       |  sr_reason_sk         INT,
       |  sr_ticket_number     LONG,
       |  sr_return_quantity   INT,
       |  sr_return_amt         decimal(7,2),
       |  sr_return_tax         decimal(7,2),
       |  sr_return_amt_inc_tax decimal(7,2),
       |  sr_fee                decimal(7,2),
       |  sr_return_ship_cost   decimal(7,2),
       |  sr_refunded_cash      decimal(7,2),
       |  sr_reversed_charge    decimal(7,2),
       |  sr_store_credit       decimal(7,2),
       |  sr_net_loss           decimal(7,2)
       |""".stripMargin

  val webSalesFields =
    s"""
       |  ws_sold_date_sk          INT,
       |  ws_sold_time_sk          INT,
       |  ws_ship_date_sk          INT,
       |  ws_item_sk               INT,
       |  ws_bill_customer_sk      INT,
       |  ws_bill_cdemo_sk         INT,
       |  ws_bill_hdemo_sk         INT,
       |  ws_bill_addr_sk          INT,
       |  ws_ship_customer_sk      INT,
       |  ws_ship_cdemo_sk         INT,
       |  ws_ship_hdemo_sk         INT,
       |  ws_ship_addr_sk          INT,
       |  ws_web_page_sk           INT,
       |  ws_web_site_sk           INT,
       |  ws_ship_mode_sk          INT,
       |  ws_warehouse_sk          INT,
       |  ws_promo_sk              INT,
       |  ws_order_number          LONG,
       |  ws_quantity              INT,
       |  ws_wholesale_cost        decimal(7,2),
       |  ws_list_price            decimal(7,2),
       |  ws_sales_price           decimal(7,2),
       |  ws_ext_discount_amt      decimal(7,2),
       |  ws_ext_sales_price       decimal(7,2),
       |  ws_ext_wholesale_cost    decimal(7,2),
       |  ws_ext_list_price        decimal(7,2),
       |  ws_ext_tax               decimal(7,2),
       |  ws_coupon_amt            decimal(7,2),
       |  ws_ext_ship_cost         decimal(7,2),
       |  ws_net_paid              decimal(7,2),
       |  ws_net_paid_inc_tax      decimal(7,2),
       |  ws_net_paid_inc_ship     decimal(7,2),
       |  ws_net_paid_inc_ship_tax decimal(7,2),
       |  ws_net_profit            decimal(7,2)
       |""".stripMargin

  val webReturnsFields =
    s"""
       |  wr_returned_date_sk       INT,
       |  wr_returned_time_sk       INT,
       |  wr_item_sk                INT,
       |  wr_refunded_customer_sk   INT,
       |  wr_refunded_cdemo_sk      INT,
       |  wr_refunded_hdemo_sk      INT,
       |  wr_refunded_addr_sk       INT,
       |  wr_returning_customer_sk  INT,
       |  wr_returning_cdemo_sk     INT,
       |  wr_returning_hdemo_sk     INT,
       |  wr_returning_addr_sk      INT,
       |  wr_web_page_sk            INT,
       |  wr_reason_sk              INT,
       |  wr_order_number           LONG,
       |  wr_return_quantity        INT,
       |  wr_return_amt             decimal(7,2),
       |  wr_return_tax             decimal(7,2),
       |  wr_return_amt_inc_tax     decimal(7,2),
       |  wr_fee                    decimal(7,2),
       |  wr_return_ship_cost       decimal(7,2),
       |  wr_refunded_cash          decimal(7,2),
       |  wr_reversed_charge        decimal(7,2),
       |  wr_account_credit         decimal(7,2),
       |  wr_net_loss               decimal(7,2)
       |""".stripMargin

  val callCenterFields =
    s"""
       |  cc_call_center_sk        INT,
       |  cc_call_center_id        STRING,
       |  cc_rec_start_date        DATE,
       |  cc_rec_end_date          DATE,
       |  cc_closed_date_sk        INT,
       |  cc_open_date_sk          INT,
       |  cc_name                  STRING,
       |  cc_class                 STRING,
       |  cc_employees             INT,
       |  cc_sq_ft                 INT,
       |  cc_hours                 STRING,
       |  cc_manager               STRING,
       |  cc_mkt_id                INT,
       |  cc_mkt_class             STRING,
       |  cc_mkt_desc              STRING,
       |  cc_market_manager        STRING,
       |  cc_division              INT,
       |  cc_division_name         STRING,
       |  cc_company               INT,
       |  cc_company_name          STRING,
       |  cc_street_number         STRING,
       |  cc_street_name           STRING,
       |  cc_street_type           STRING,
       |  cc_suite_number          STRING,
       |  cc_city                  STRING,
       |  cc_county                STRING,
       |  cc_state                 STRING,
       |  cc_zip                   STRING,
       |  cc_country               STRING,
       |  cc_gmt_offset            decimal(5,2),
       |  cc_tax_percentage        decimal(5,2)
       |""".stripMargin

  val catalogPageFields =
    s"""
       |  cp_catalog_page_sk       int,
       |  cp_catalog_page_id       string,
       |  cp_start_date_sk         int,
       |  cp_end_date_sk           int,
       |  cp_department            string,
       |  cp_catalog_number        int,
       |  cp_catalog_page_number   int,
       |  cp_description           string,
       |  cp_type                  string
       |""".stripMargin

  val customerFields =
    s"""
       |  c_customer_sk             int,
       |  c_customer_id             string,
       |  c_current_cdemo_sk        int,
       |  c_current_hdemo_sk        int,
       |  c_current_addr_sk         int,
       |  c_first_shipto_date_sk    int,
       |  c_first_sales_date_sk     int,
       |  c_salutation              string,
       |  c_first_name              string,
       |  c_last_name               string,
       |  c_preferred_cust_flag     string,
       |  c_birth_day               int,
       |  c_birth_month             int,
       |  c_birth_year              int,
       |  c_birth_country           string,
       |  c_login                   string,
       |  c_email_address           string,
       |  c_last_review_date        string
       |""".stripMargin

  val customerAddressFields =
    s"""
       |  ca_address_sk             int,
       |  ca_address_id             string,
       |  ca_street_number          string,
       |  ca_street_name            string,
       |  ca_street_type            string,
       |  ca_suite_number           string,
       |  ca_city                   string,
       |  ca_county                 string,
       |  ca_state                  string,
       |  ca_zip                    string,
       |  ca_country                string,
       |  ca_gmt_offset             decimal(5,2),
       |  ca_location_type          string
       |""".stripMargin

  val customerDemographicsFields =
    s"""
       |  cd_demo_sk                int,
       |  cd_gender                 string,
       |  cd_marital_status         string,
       |  cd_education_status       string,
       |  cd_purchase_estimate      int,
       |  cd_credit_rating          string,
       |  cd_dep_count              int,
       |  cd_dep_employed_count     int,
       |  cd_dep_college_count      int
       |""".stripMargin

  val dateDimFields =
    s"""
       |  d_date_sk                 int,
       |  d_date_id                 string,
       |  d_date                    date,
       |  d_month_seq               int,
       |  d_week_seq                int,
       |  d_quarter_seq             int,
       |  d_year                    int,
       |  d_dow                     int,
       |  d_moy                     int,
       |  d_dom                     int,
       |  d_qoy                     int,
       |  d_fy_year                 int,
       |  d_fy_quarter_seq          int,
       |  d_fy_week_seq             int,
       |  d_day_name                string,
       |  d_quarter_name            string,
       |  d_holiday                 string,
       |  d_weekend                 string,
       |  d_following_holiday       string,
       |  d_first_dom               int,
       |  d_last_dom                int,
       |  d_same_day_ly             int,
       |  d_same_day_lq             int,
       |  d_current_day             string,
       |  d_current_week            string,
       |  d_current_month           string,
       |  d_current_quarter         string,
       |  d_current_year            string
       |""".stripMargin

  val householdDemographicsFields =
    s"""
       |  hd_demo_sk                int,
       |  hd_income_band_sk         int,
       |  hd_buy_potential          string,
       |  hd_dep_count              int,
       |  hd_vehicle_count          int
       |""".stripMargin

  val incomeBandFields =
    s"""
       |  ib_income_band_sk         int,
       |  ib_lower_bound            int,
       |  ib_upper_bound            int
       |""".stripMargin

  val itemFields =
    s"""
       |  i_item_sk                 int,
       |  i_item_id                 string,
       |  i_rec_start_date          date,
       |  i_rec_end_date            date,
       |  i_item_desc               string,
       |  i_current_price           decimal(7,2),
       |  i_wholesale_cost          decimal(7,2),
       |  i_brand_id                int,
       |  i_brand                   string,
       |  i_class_id                int,
       |  i_class                   string,
       |  i_category_id             int,
       |  i_category                string,
       |  i_manufact_id             int,
       |  i_manufact                string,
       |  i_size                    string,
       |  i_formulation             string,
       |  i_color                   string,
       |  i_units                   string,
       |  i_container               string,
       |  i_manager_id              int,
       |  i_product_name            string
       |""".stripMargin

  val promotionFields =
    s"""
       |  p_promo_sk                int,
       |  p_promo_id                string,
       |  p_start_date_sk           int,
       |  p_end_date_sk             int,
       |  p_item_sk                 int,
       |  p_cost                    decimal(15,2),
       |  p_response_target         int,
       |  p_promo_name              string,
       |  p_channel_dmail           string,
       |  p_channel_email           string,
       |  p_channel_catalog         string,
       |  p_channel_tv              string,
       |  p_channel_radio           string,
       |  p_channel_press           string,
       |  p_channel_event           string,
       |  p_channel_demo            string,
       |  p_channel_details         string,
       |  p_purpose                 string,
       |  p_discount_active         string
       |""".stripMargin

  val reasonFields =
    s"""
       |  r_reason_sk               int,
       |  r_reason_id               string,
       |  r_reason_desc             string
       |""".stripMargin

  val shipModeFields =
    s"""
       |  sm_ship_mode_sk           int,
       |  sm_ship_mode_id           string,
       |  sm_type                   string,
       |  sm_code                   string,
       |  sm_carrier                string,
       |  sm_contract               string
       |""".stripMargin

  val storeFields =
    s"""
       |  s_store_sk                int,
       |  s_store_id                string,
       |  s_rec_start_date          date,
       |  s_rec_end_date            date,
       |  s_closed_date_sk          int,
       |  s_store_name              string,
       |  s_number_employees        int,
       |  s_floor_space             int,
       |  s_hours                   string,
       |  s_manager                 string,
       |  s_market_id               int,
       |  s_geography_class         string,
       |  s_market_desc             string,
       |  s_market_manager          string,
       |  s_division_id             int,
       |  s_division_name           string,
       |  s_company_id              int,
       |  s_company_name            string,
       |  s_street_number           string,
       |  s_street_name             string,
       |  s_street_type             string,
       |  s_suite_number            string,
       |  s_city                    string,
       |  s_county                  string,
       |  s_state                   string,
       |  s_zip                     string,
       |  s_country                 string,
       |  s_gmt_offset              decimal(5,2),
       |  s_tax_precentage          decimal(5,2)
       |""".stripMargin

  val timeDimFields =
    s"""
       |  t_time_sk                 int,
       |  t_time_id                 string,
       |  t_time                    int,
       |  t_hour                    int,
       |  t_minute                  int,
       |  t_second                  int,
       |  t_am_pm                   string,
       |  t_shift                   string,
       |  t_sub_shift               string,
       |  t_meal_time               string
       |""".stripMargin

  val warehouseFields =
    s"""
       |  w_warehouse_sk           int,
       |  w_warehouse_id           string,
       |  w_warehouse_name         string,
       |  w_warehouse_sq_ft        int,
       |  w_street_number          string,
       |  w_street_name            string,
       |  w_street_type            string,
       |  w_suite_number           string,
       |  w_city                   string,
       |  w_county                 string,
       |  w_state                  string,
       |  w_zip                    string,
       |  w_country                string,
       |  w_gmt_offset             decimal(5,2)
       |""".stripMargin

  val webPageFields =
    s"""
       |  wp_web_page_sk           int,
       |  wp_web_page_id           string,
       |  wp_rec_start_date        date,
       |  wp_rec_end_date          date,
       |  wp_creation_date_sk      int,
       |  wp_access_date_sk        int,
       |  wp_autogen_flag          string,
       |  wp_customer_sk           int,
       |  wp_url                   string,
       |  wp_type                  string,
       |  wp_char_count            int,
       |  wp_link_count            int,
       |  wp_image_count           int,
       |  wp_max_ad_count          int
       |""".stripMargin

  val webSiteFields =
    s"""
       |  web_site_sk              int,
       |  web_site_id              string,
       |  web_rec_start_date       date,
       |  web_rec_end_date         date,
       |  web_name                 string,
       |  web_open_date_sk         int,
       |  web_close_date_sk        int,
       |  web_class                string,
       |  web_manager              string,
       |  web_mkt_id               int,
       |  web_mkt_class            string,
       |  web_mkt_desc             string,
       |  web_market_manager       string,
       |  web_company_id           int,
       |  web_company_name         string,
       |  web_street_number        string,
       |  web_street_name          string,
       |  web_street_type          string,
       |  web_suite_number         string,
       |  web_city                 string,
       |  web_county               string,
       |  web_state                string,
       |  web_zip                  string,
       |  web_country              string,
       |  web_gmt_offset           decimal(5,2),
       |  web_tax_percentage       decimal(5,2)
       |""".stripMargin

  def main(args: Array[String]): Unit = {}

  def genTPCDSMergeTreeTables(
      dbName: String,
      dataPathRoot: String,
      tablePrefix: String,
      tableSuffix: String): Unit = {}

  def genTPCDSParquetTables(
      dbName: String,
      dataPathRoot: String,
      tablePrefix: String,
      tableSuffix: String): ArrayBuffer[String] = {
    val catalogSalesTbl = "catalog_sales"
    val catalogSalesPartitionCols = "PARTITIONED BY (cs_sold_date_sk)"

    val catalogReturnsTbl = "catalog_returns"
    val catalogReturnsPartitionCols = "PARTITIONED BY (cr_returned_date_sk)"

    val inventoryTbl = "inventory"
    val inventoryPartitionCols = "PARTITIONED BY (inv_date_sk)"

    val storeSalesTbl = "store_sales"
    val storeSalesPartitionCols = "PARTITIONED BY (ss_sold_date_sk)"

    val storeReturnsTbl = "store_returns"
    val storeReturnsPartitionCols = "PARTITIONED BY (sr_returned_date_sk)"

    val webSalesTbl = "web_sales"
    val webSalesPartitionCols = "PARTITIONED BY (ws_sold_date_sk)"

    val webReturnsTbl = "web_returns"
    val webReturnsPartitionCols = "PARTITIONED BY (wr_returned_date_sk)"

    val callCenterTbl = "call_center"
    val callCenterPartitionCols = ""

    val catalogPageTbl = "catalog_page"
    val catalogPagePartitionCols = ""

    val customerTbl = "customer"
    val customerPartitionCols = ""

    val customerAddressTbl = "customer_address"
    val customerAddressPartitionCols = ""

    val customerDemographicsTbl = "customer_demographics"
    val customerDemographicsPartitionCols = ""

    val dateDimTbl = "date_dim"
    val dateDimPartitionCols = ""

    val householdDemographicsTbl = "household_demographics"
    val householdDemographicsPartitionCols = ""

    val incomeBandTbl = "income_band"
    val incomeBandPartitionCols = ""

    val itemTbl = "item"
    val itemPartitionCols = ""

    val promotionTbl = "promotion"
    val promotionPartitionCols = ""

    val reasonTbl = "reason"
    val reasonPartitionCols = ""

    val shipModeTbl = "ship_mode"
    val shipModePartitionCols = ""

    val storeTbl = "store"
    val storePartitionCols = ""

    val timeDimTbl = "time_dim"
    val timeDimPartitionCols = ""

    val warehouseTbl = "warehouse"
    val warehousePartitionCols = ""

    val webPageTbl = "web_page"
    val webPagePartitionCols = ""

    val webSiteTbl = "web_site"
    val webSitePartitionCols = ""

    val res = new ArrayBuffer[String]()
    res +=
      s"""
         |CREATE DATABASE IF NOT EXISTS $dbName
         |WITH DBPROPERTIES (engine='Parquet');
         |""".stripMargin

    res += s"""use $dbName;"""

    // catalog_sales
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      catalogSalesTbl,
      catalogSalesFields,
      catalogSalesPartitionCols,
      tablePrefix,
      tableSuffix)

    // catalog_returns
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      catalogReturnsTbl,
      catalogReturnsFields,
      catalogReturnsPartitionCols,
      tablePrefix,
      tableSuffix)

    // inventory
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      inventoryTbl,
      inventoryFields,
      inventoryPartitionCols,
      tablePrefix,
      tableSuffix)

    // store_sales
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      storeSalesTbl,
      storeSalesFields,
      storeSalesPartitionCols,
      tablePrefix,
      tableSuffix)

    // store_returns
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      storeReturnsTbl,
      storeReturnsFields,
      storeReturnsPartitionCols,
      tablePrefix,
      tableSuffix)

    // web_sales
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      webSalesTbl,
      webSalesFields,
      webSalesPartitionCols,
      tablePrefix,
      tableSuffix)

    // web_returns
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      webReturnsTbl,
      webReturnsFields,
      webReturnsPartitionCols,
      tablePrefix,
      tableSuffix)

    // call_center
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      callCenterTbl,
      callCenterFields,
      callCenterPartitionCols,
      tablePrefix,
      tableSuffix)

    // catalog_page
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      catalogPageTbl,
      catalogPageFields,
      catalogPagePartitionCols,
      tablePrefix,
      tableSuffix)

    // customer
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      customerTbl,
      customerFields,
      customerPartitionCols,
      tablePrefix,
      tableSuffix)

    // customer_address
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      customerAddressTbl,
      customerAddressFields,
      customerAddressPartitionCols,
      tablePrefix,
      tableSuffix)

    // customer_demographics
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      customerDemographicsTbl,
      customerDemographicsFields,
      customerDemographicsPartitionCols,
      tablePrefix,
      tableSuffix)

    // date_dim
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      dateDimTbl,
      dateDimFields,
      dateDimPartitionCols,
      tablePrefix,
      tableSuffix)

    // household_demographics
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      householdDemographicsTbl,
      householdDemographicsFields,
      householdDemographicsPartitionCols,
      tablePrefix,
      tableSuffix)

    // income_band
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      incomeBandTbl,
      incomeBandFields,
      incomeBandPartitionCols,
      tablePrefix,
      tableSuffix)

    // item
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      itemTbl,
      itemFields,
      itemPartitionCols,
      tablePrefix,
      tableSuffix)

    // promotion
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      promotionTbl,
      promotionFields,
      promotionPartitionCols,
      tablePrefix,
      tableSuffix)

    // reason
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      reasonTbl,
      reasonFields,
      reasonPartitionCols,
      tablePrefix,
      tableSuffix)

    // ship_mode
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      shipModeTbl,
      shipModeFields,
      shipModePartitionCols,
      tablePrefix,
      tableSuffix)

    // store
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      storeTbl,
      storeFields,
      storePartitionCols,
      tablePrefix,
      tableSuffix)

    // time_dim
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      timeDimTbl,
      timeDimFields,
      timeDimPartitionCols,
      tablePrefix,
      tableSuffix)

    // warehouse
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      warehouseTbl,
      warehouseFields,
      warehousePartitionCols,
      tablePrefix,
      tableSuffix)

    // web_page
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      webPageTbl,
      webPageFields,
      webPagePartitionCols,
      tablePrefix,
      tableSuffix)

    // web_site
    genOneTPCDSParquetTableSQL(
      res,
      dataPathRoot,
      webSiteTbl,
      webSiteFields,
      webSitePartitionCols,
      tablePrefix,
      tableSuffix)

    // scalastyle:off println
    println(res.mkString("\n\n"))
    // scalastyle:on println
    res
  }

  def genOneTPCDSParquetTableSQL(
      res: ArrayBuffer[String],
      dataPathRoot: String,
      tblName: String,
      tblFields: String,
      tblPartitionCols: String,
      tablePrefix: String,
      tableSuffix: String): Unit = {
    // scalastyle:off println
    println(s"start to generate sqls for table $tblName")
    // scalastyle:on println
    res += s"""DROP TABLE IF EXISTS $tablePrefix$tblName$tableSuffix;"""
    res +=
      s"""
         |CREATE TABLE IF NOT EXISTS $tablePrefix$tblName$tableSuffix (
         |$tblFields
         | )
         | USING PARQUET
         | $tblPartitionCols
         | LOCATION '${dataPathRoot + tblName}'
         | ;
         |""".stripMargin

    if (!tblPartitionCols.isEmpty) {
      res += s"""MSCK REPAIR TABLE $tablePrefix$tblName$tableSuffix;"""
    }
  }

  def genTPCDSCSV2ParquetSQL(
      dbName: String,
      parquetDbName: String,
      csvPathRoot: String,
      parquetPathRoot: String,
      tablePrefix: String,
      tableSuffix: String): ArrayBuffer[String] = {
    val catalogSalesTbl = "catalog_sales"
    val catalogSalesParts = "/*+ REPARTITION(2) */"
    val catalogSalesPartitionCols = "PARTITIONED BY (cs_sold_date_sk)"

    val catalogReturnsTbl = "catalog_returns"
    val catalogReturnsParts = ""
    val catalogReturnsPartitionCols = "PARTITIONED BY (cr_returned_date_sk)"

    val inventoryTbl = "inventory"
    val inventoryParts = "/*+ REPARTITION(2) */"
    val inventoryPartitionCols = "PARTITIONED BY (inv_date_sk)"

    val storeSalesTbl = "store_sales"
    val storeSalesParts = "/*+ REPARTITION(2) */"
    val storeSalesPartitionCols = "PARTITIONED BY (ss_sold_date_sk)"

    val storeReturnsTbl = "store_returns"
    val storeReturnsParts = ""
    val storeReturnsPartitionCols = "PARTITIONED BY (sr_returned_date_sk)"

    val webSalesTbl = "web_sales"
    val webSalesParts = ""
    val webSalesPartitionCols = "PARTITIONED BY (ws_sold_date_sk)"

    val webReturnsTbl = "web_returns"
    val webReturnsParts = ""
    val webReturnsPartitionCols = "PARTITIONED BY (wr_returned_date_sk)"

    val callCenterTbl = "call_center"
    val callCenterParts = ""
    val callCenterPartitionCols = ""

    val catalogPageTbl = "catalog_page"
    val catalogPageParts = ""
    val catalogPagePartitionCols = ""

    val customerTbl = "customer"
    val customerParts = ""
    val customerPartitionCols = ""

    val customerAddressTbl = "customer_address"
    val customerAddressParts = ""
    val customerAddressPartitionCols = ""

    val customerDemographicsTbl = "customer_demographics"
    val customerDemographicsParts = ""
    val customerDemographicsPartitionCols = ""

    val dateDimTbl = "date_dim"
    val dateDimParts = ""
    val dateDimPartitionCols = ""

    val householdDemographicsTbl = "household_demographics"
    val householdDemographicsParts = ""
    val householdDemographicsPartitionCols = ""

    val incomeBandTbl = "income_band"
    val incomeBandParts = ""
    val incomeBandPartitionCols = ""

    val itemTbl = "item"
    val itemParts = ""
    val itemPartitionCols = ""

    val promotionTbl = "promotion"
    val promotionParts = ""
    val promotionPartitionCols = ""

    val reasonTbl = "reason"
    val reasonParts = ""
    val reasonPartitionCols = ""

    val shipModeTbl = "ship_mode"
    val shipModeParts = ""
    val shipModePartitionCols = ""

    val storeTbl = "store"
    val storeParts = ""
    val storePartitionCols = ""

    val timeDimTbl = "time_dim"
    val timeDimParts = ""
    val timeDimPartitionCols = ""

    val warehouseTbl = "warehouse"
    val warehouseParts = ""
    val warehousePartitionCols = ""

    val webPageTbl = "web_page"
    val webPageParts = ""
    val webPagePartitionCols = ""

    val webSiteTbl = "web_site"
    val webSiteParts = ""
    val webSitePartitionCols = ""

    val res = new ArrayBuffer[String]()
    res +=
      s"""
         |CREATE DATABASE IF NOT EXISTS $parquetDbName
         |WITH DBPROPERTIES (engine='Parquet');
         |""".stripMargin

    res += s"""CREATE DATABASE IF NOT EXISTS $dbName;"""
    res += s"""use $dbName;"""

    // catalog_sales
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      catalogSalesTbl,
      catalogSalesFields,
      catalogSalesPartitionCols,
      catalogSalesParts,
      tablePrefix,
      tableSuffix)

    // catalog_returns
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      catalogReturnsTbl,
      catalogReturnsFields,
      catalogReturnsPartitionCols,
      catalogReturnsParts,
      tablePrefix,
      tableSuffix)

    // inventory
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      inventoryTbl,
      inventoryFields,
      inventoryPartitionCols,
      inventoryParts,
      tablePrefix,
      tableSuffix)

    // store_sales
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      storeSalesTbl,
      storeSalesFields,
      storeSalesPartitionCols,
      storeSalesParts,
      tablePrefix,
      tableSuffix)

    // store_returns
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      storeReturnsTbl,
      storeReturnsFields,
      storeReturnsPartitionCols,
      storeReturnsParts,
      tablePrefix,
      tableSuffix)

    // web_sales
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      webSalesTbl,
      webSalesFields,
      webSalesPartitionCols,
      webSalesParts,
      tablePrefix,
      tableSuffix)

    // web_returns
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      webReturnsTbl,
      webReturnsFields,
      webReturnsPartitionCols,
      webReturnsParts,
      tablePrefix,
      tableSuffix)

    // call_center
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      callCenterTbl,
      callCenterFields,
      callCenterPartitionCols,
      callCenterParts,
      tablePrefix,
      tableSuffix)

    // catalog_page
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      catalogPageTbl,
      catalogPageFields,
      catalogPagePartitionCols,
      catalogPageParts,
      tablePrefix,
      tableSuffix)

    // customer
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      customerTbl,
      customerFields,
      customerPartitionCols,
      customerParts,
      tablePrefix,
      tableSuffix)

    // customer_address
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      customerAddressTbl,
      customerAddressFields,
      customerAddressPartitionCols,
      customerAddressParts,
      tablePrefix,
      tableSuffix)

    // customer_demographics
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      customerDemographicsTbl,
      customerDemographicsFields,
      customerDemographicsPartitionCols,
      customerDemographicsParts,
      tablePrefix,
      tableSuffix
    )

    // date_dim
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      dateDimTbl,
      dateDimFields,
      dateDimPartitionCols,
      dateDimParts,
      tablePrefix,
      tableSuffix)

    // household_demographics
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      householdDemographicsTbl,
      householdDemographicsFields,
      householdDemographicsPartitionCols,
      householdDemographicsParts,
      tablePrefix,
      tableSuffix
    )

    // income_band
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      incomeBandTbl,
      incomeBandFields,
      incomeBandPartitionCols,
      incomeBandParts,
      tablePrefix,
      tableSuffix)

    // item
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      itemTbl,
      itemFields,
      itemPartitionCols,
      itemParts,
      tablePrefix,
      tableSuffix)

    // promotion
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      promotionTbl,
      promotionFields,
      promotionPartitionCols,
      promotionParts,
      tablePrefix,
      tableSuffix)

    // reason
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      reasonTbl,
      reasonFields,
      reasonPartitionCols,
      reasonParts,
      tablePrefix,
      tableSuffix)

    // ship_mode
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      shipModeTbl,
      shipModeFields,
      shipModePartitionCols,
      shipModeParts,
      tablePrefix,
      tableSuffix)

    // store
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      storeTbl,
      storeFields,
      storePartitionCols,
      storeParts,
      tablePrefix,
      tableSuffix)

    // time_dim
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      timeDimTbl,
      timeDimFields,
      timeDimPartitionCols,
      timeDimParts,
      tablePrefix,
      tableSuffix)

    // warehouse
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      warehouseTbl,
      warehouseFields,
      warehousePartitionCols,
      warehouseParts,
      tablePrefix,
      tableSuffix)

    // web_page
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      webPageTbl,
      webPageFields,
      webPagePartitionCols,
      webPageParts,
      tablePrefix,
      tableSuffix)

    // web_site
    genOneTPCDSCSV2ParquetSQL(
      res,
      csvPathRoot,
      parquetPathRoot,
      webSiteTbl,
      webSiteFields,
      webSitePartitionCols,
      webSiteParts,
      tablePrefix,
      tableSuffix)

    // scalastyle:off println
    println(res.mkString("\n\n"))
    // scalastyle:on println
    res
  }

  def genOneTPCDSCSV2ParquetSQL(
      res: ArrayBuffer[String],
      csvPathRoot: String,
      parquetPathRoot: String,
      tblName: String,
      tblFields: String,
      tblPartitionCols: String,
      tblParts: String,
      tablePrefix: String,
      tableSuffix: String): Unit = {
    // scalastyle:off println
    println(s"start to generate sqls for table $tblName")
    // scalastyle:on println
    res += s"""DROP TABLE IF EXISTS ${tblName}_csv;"""
    res +=
      s"""
         |CREATE TABLE IF NOT EXISTS ${tblName}_csv (
         |$tblFields
         | )
         | USING csv
         | OPTIONS (
         |  path '${csvPathRoot + tblName + "/"}',
         |  header false,
         |  sep '|'
         | );
         |""".stripMargin
    res += s"""DROP TABLE IF EXISTS $tablePrefix$tblName$tableSuffix;"""
    res +=
      s"""
         |CREATE TABLE IF NOT EXISTS $tablePrefix$tblName$tableSuffix
         | USING PARQUET
         | $tblPartitionCols
         | LOCATION '${parquetPathRoot + tblName}'
         | AS SELECT $tblParts * FROM ${tblName}_csv;
         |""".stripMargin
  }

}
