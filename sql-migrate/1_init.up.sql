CREATE TABLE "operator" (
    "id" bigserial PRIMARY KEY,
    "phone" VARCHAR(16) NOT NULL,
    "surname" VARCHAR(32) NOT NULL,
    "first_name" VARCHAR(32) NOT NULL,
    "created_at" timestamptz NOT NULL DEFAULT (now())
);

SELECT 1 FROM operator WHERE phone = '7999';