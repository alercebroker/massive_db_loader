CREATE TABLE detections (
    candid VARCHAR(255) PRIMARY KEY,
    oid VARCHAR(255) REFERENCES object(oid),
    mag INT
)
