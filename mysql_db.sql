CREATE TABLE `demo` (
  `transactionID` bigint NOT NULL,
  `ACR` varchar(255) DEFAULT NULL,
  `amount` double(11,2) DEFAULT '0.00',
  PRIMARY KEY (`transactionID`)
);