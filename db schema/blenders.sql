CREATE TABLE "zones" (
  "id" SERIAL PRIMARY KEY,
  "name" string,
  "division_id" pk
);

CREATE TABLE "divisions" (
  "id" SERIAL PRIMARY KEY,
  "name" pk
);

CREATE TABLE "companies" (
  "id" SERIAL PRIMARY KEY,
  "administrator_id" pk,
  "name" string
);

CREATE TABLE "customers" (
  "id" SERIAL PRIMARY KEY,
  "name" string,
  "email" string
);

CREATE TABLE "profiles" (
  "id" SERIAL PRIMARY KEY,
  "user_id" id,
  "phone" string,
  "address1" string,
  "address2" string,
  "zipcode" string,
  "city" string,
  "state" string,
  "country" string
);

CREATE TABLE "machines" (
  "id" SERIAL PRIMARY KEY,
  "name" string,
  "customer_id" pk,
  "zone_id" pk,
  "division_id" pk
);

CREATE TABLE "devices" (
  "id" SERIAL PRIMARY KEY,
  "machine_id" pk
);

CREATE TABLE "blenders" (
  "id" SERIAL PRIMARY KEY,
  "tag_id" pk,
  "values" json,
  "timestamp" timestamp
);

ALTER TABLE "machines" ADD FOREIGN KEY ("id") REFERENCES "devices" ("machine_id");

ALTER TABLE "customers" ADD FOREIGN KEY ("id") REFERENCES "companies" ("administrator_id");

ALTER TABLE "customers" ADD FOREIGN KEY ("id") REFERENCES "profiles" ("user_id");

ALTER TABLE "machines" ADD FOREIGN KEY ("customer_id") REFERENCES "customers" ("id");

ALTER TABLE "divisions" ADD FOREIGN KEY ("id") REFERENCES "machines" ("division_id");

ALTER TABLE "zones" ADD FOREIGN KEY ("division_id") REFERENCES "divisions" ("id");
