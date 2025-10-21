CREATE TABLE "dev_practice_db"."demo" (
  "whId" "pg_catalog"."int8" NOT NULL DEFAULT nextval('"dev_practice_db".demo_whid_seq'::regclass),
  "dbId" "pg_catalog"."int4" NOT NULL,
  "transactionID" "pg_catalog"."int8",
  "ACR" "pg_catalog"."varchar" COLLATE "pg_catalog"."default",
  "amount" "pg_catalog"."float8",
  CONSTRAINT "demo_pkey" PRIMARY KEY ("whId"),
  CONSTRAINT "demo_transactionid_pk" UNIQUE ("transactionID","dbId")
);