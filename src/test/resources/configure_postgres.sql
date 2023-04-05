-- Mocking up the DECS Metadata RDS for testing
DROP TABLE IF EXISTS metadata.document_metadata;
DROP SCHEMA IF EXISTS metadata CASCADE;

CREATE SCHEMA metadata;
CREATE TABLE metadata.document_metadata  (
    document_id varchar(64),
    uploaded_date timestamp without time zone,
    relevant_document varchar(1),
    s3_key varchar(1024)
);

INSERT INTO metadata.document_metadata
VALUES
    ('a1', timestamp '2023-03-22 12:00:00', 'Y', 'http://localhost:9000/decs-bucket/decs-file1.pdf'),
    ('b2', timestamp '2023-03-22 13:00:00', 'Y', 'http://localhost:9000/decs-bucket/decs-file2.pdf'),
    ('c3', timestamp '2023-03-22 14:00:00', 'Y', 'http://localhost:9000/decs-bucket/decs-file3.pdf'),
    ('d4', timestamp '2023-03-22 15:00:00', 'N', 'http://localhost:9000/decs-bucket/decs-file4.pdf');
