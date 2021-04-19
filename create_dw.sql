DROP SCHEMA IF EXISTS `dw` ;
CREATE SCHEMA IF NOT EXISTS `dw` ;
USE `dw` ;
DROP TABLE IF EXISTS `dw`.`dim_date` ;
CREATE TABLE IF NOT EXISTS `dw`.`dim_date` (  `date_id` INT NOT NULL AUTO_INCREMENT,  `day` TINYINT(2) NOT NULL,  `week` TINYINT(2) NOT NULL,  `month` TINYINT(2) NOT NULL,  `quarter` TINYINT(1) NOT NULL,  `year` SMALLINT(4) NOT NULL,  PRIMARY KEY (`date_id`))ENGINE = InnoDB;
DROP TABLE IF EXISTS `dw`.`dim_location` ;
CREATE TABLE IF NOT EXISTS `dw`.`dim_location` (  `location_id` INT NOT NULL,  `city` VARCHAR(100) NOT NULL,  `state` VARCHAR(100) NOT NULL,  PRIMARY KEY (`location_id`))ENGINE = InnoDB;
DROP TABLE IF EXISTS `dw`.`dim_company` ;
CREATE TABLE IF NOT EXISTS `dw`.`dim_company` (  `stock_symbol` VARCHAR(5) NOT NULL,  `company_name` VARCHAR(100) NOT NULL,  `sector` VARCHAR(100) NOT NULL,  `industry` VARCHAR(100) NOT NULL,  PRIMARY KEY (`stock_symbol`))ENGINE = InnoDB;
DROP TABLE IF EXISTS `dw`.`fact_daily_stock` ;
CREATE TABLE IF NOT EXISTS `dw`.`fact_daily_stock` (  `fact_id` INT NOT NULL AUTO_INCREMENT,  `date_id` INT NOT NULL,  `stock_symbol` VARCHAR(5) NOT NULL,  `location_id` INT NOT NULL,  `open` DECIMAL(15,6) NOT NULL,  `close` DECIMAL(15,6) NOT NULL,  `high` DECIMAL(15,6) NOT NULL,  `low` DECIMAL(15,6) NOT NULL,  `volume` BIGINT NOT NULL,  `sentiment` VARCHAR(50) NULL,  PRIMARY KEY (`fact_id`),  INDEX `fk_date_id_idx` (`date_id` ASC) VISIBLE,  INDEX `fk_location_id_idx` (`location_id` ASC) VISIBLE,  INDEX `fk_stock_symbol_idx` (`stock_symbol` ASC) VISIBLE,  CONSTRAINT `fk_d_date_id`    FOREIGN KEY (`date_id`)    REFERENCES `dw`.`dim_date` (`date_id`)    ON DELETE CASCADE    ON UPDATE CASCADE,  CONSTRAINT `fk_d_location_id`    FOREIGN KEY (`location_id`)    REFERENCES `dw`.`dim_location` (`location_id`)    ON DELETE CASCADE    ON UPDATE CASCADE,  CONSTRAINT `fk_d_stock_symbol`    FOREIGN KEY (`stock_symbol`)    REFERENCES `dw`.`dim_company` (`stock_symbol`)    ON DELETE NO ACTION    ON UPDATE NO ACTION)ENGINE = InnoDB;
DROP TABLE IF EXISTS `dw`.`dim_quarter` ;
CREATE TABLE IF NOT EXISTS `dw`.`dim_quarter` (  `quarter_id` INT NOT NULL AUTO_INCREMENT,  `quarter` TINYINT(1) NOT NULL,  `year` SMALLINT(4) NOT NULL,  PRIMARY KEY (`quarter_id`))ENGINE = InnoDB;
DROP TABLE IF EXISTS `dw`.`fact_quarterly_stock` ;
CREATE TABLE IF NOT EXISTS `dw`.`fact_quarterly_stock` (  `fact_id` INT NOT NULL AUTO_INCREMENT,  `quarter_id` INT NOT NULL,  `stock_symbol` VARCHAR(5) NOT NULL,  `location_id` INT NOT NULL,  `revenue` DECIMAL(6,2) NOT NULL,  `open` DECIMAL(15,6) NOT NULL,  `close` DECIMAL(15,6) NOT NULL,  `high` DECIMAL(15,6) NOT NULL,  `low` DECIMAL(15,6) NOT NULL,  `volume` BIGINT NOT NULL,  PRIMARY KEY (`fact_id`),  INDEX `fk_quarter_id_idx` (`quarter_id` ASC) VISIBLE,  INDEX `fk_location_id_idx` (`location_id` ASC) VISIBLE,  INDEX `fk_stock_symbol_idx` (`stock_symbol` ASC) VISIBLE,  CONSTRAINT `fk_q_quarter_id`    FOREIGN KEY (`quarter_id`)    REFERENCES `dw`.`dim_quarter` (`quarter_id`)    ON DELETE CASCADE    ON UPDATE CASCADE,  CONSTRAINT `fk_q_location_id`    FOREIGN KEY (`location_id`)    REFERENCES `dw`.`dim_location` (`location_id`)    ON DELETE CASCADE    ON UPDATE CASCADE,  CONSTRAINT `fk_q_stock_symbol`    FOREIGN KEY (`stock_symbol`)    REFERENCES `dw`.`dim_company` (`stock_symbol`)    ON DELETE NO ACTION    ON UPDATE NO ACTION)ENGINE = InnoDB;
DROP TABLE IF EXISTS `dw`.`fact_market_sentiment` ;
CREATE TABLE IF NOT EXISTS `dw`.`fact_market_sentiment` (  `sentiment_id` INT NOT NULL AUTO_INCREMENT,  `stock_symbol` VARCHAR(5) NOT NULL,  `bearish` varchar(20) NOT NULL,  `bullish` varchar(20) NOT NULL,  `unemotional` varchar(20) NOT NULL,  PRIMARY KEY (`sentiment_id`),  INDEX `fk_tw_stock_symbol_idx` (`stock_symbol` ASC) VISIBLE,  CONSTRAINT `fk_qtw_stock_symbol`    FOREIGN KEY (`stock_symbol`)    REFERENCES `dw`.`dim_company` (`stock_symbol`)    ON DELETE NO ACTION    ON UPDATE NO ACTION)ENGINE = InnoDB;
DROP TABLE IF EXISTS `dw`.`agg_stock_volume` ;
CREATE TABLE `dw`.`agg_stock_volume` (`agg_id` int NOT NULL AUTO_INCREMENT,`date_id` int NOT NULL,`location_id` int NOT NULL,`volume` bigint NOT NULL, PRIMARY KEY (`agg_id`), INDEX `fk_agg_date_id_idx` (`date_id` ASC) VISIBLE, INDEX `fk_agg_location_id_idx` (`location_id` ASC) VISIBLE, CONSTRAINT `fk_agg_d_date_id`    FOREIGN KEY (`date_id`)    REFERENCES `dw`.`dim_date` (`date_id`)    ON DELETE CASCADE    ON UPDATE CASCADE, CONSTRAINT `fk_agg_d_location_id`    FOREIGN KEY (`location_id`)    REFERENCES `dw`.`dim_location` (`location_id`)    ON DELETE CASCADE    ON UPDATE CASCADE ) ENGINE=InnoDB;
DROP TABLE IF EXISTS `dw`.`agg_quaterly_revenue` ;
CREATE TABLE `dw`.`agg_quaterly_revenue` (`agg_id` int NOT NULL AUTO_INCREMENT,`quarter_id` int NOT NULL,`stock_symbol` varchar(5) NOT NULL,`revenue` decimal(6,2) NOT NULL, PRIMARY KEY (`agg_id`), INDEX `fk_agg_quarter_id_idx` (`quarter_id` ASC) VISIBLE, INDEX `fk_agg_stock_symbol_idx` (`stock_symbol` ASC) VISIBLE, CONSTRAINT `fk_agg_q_quarter_id`    FOREIGN KEY (`quarter_id`)    REFERENCES `dw`.`dim_quarter` (`quarter_id`)    ON DELETE CASCADE    ON UPDATE CASCADE, CONSTRAINT `fk_agg_q_stock_symbol`    FOREIGN KEY (`stock_symbol`)    REFERENCES `dw`.`dim_company` (`stock_symbol`)    ON DELETE NO ACTION    ON UPDATE NO ACTION) ENGINE=InnoDB;
