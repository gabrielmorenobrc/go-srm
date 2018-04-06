CREATE TABLE master1 (
  id   BIGINT       NOT NULL,
  name VARCHAR(255) NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE master2 (
  id   BIGINT       NOT NULL,
  name VARCHAR(255) NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE detail (
  id         BIGINT       NOT NULL,
  master1_id BIGINT       NOT NULL,
  master2_id BIGINT       NOT NULL,
  name       VARCHAR(255) NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (master1_id) REFERENCES master1 (id),
  FOREIGN KEY (master2_id) REFERENCES master2 (id)
);

CREATE TABLE yetanother (
  id         BIGINT       NOT NULL,
  detail_id BIGINT       NOT NULL,
  name       VARCHAR(255) NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (detail_id) REFERENCES detail (id)
);

INSERT INTO master1 VALUES (1, 'One');
INSERT INTO master1 VALUES (2, 'Two');
INSERT INTO master2 VALUES (1, 'A');
INSERT INTO master2 VALUES (2, 'B');

INSERT INTO detail VALUES (1, 1, 1, '1 1');
INSERT INTO detail VALUES (2, 1, 2, '1 2');
INSERT INTO detail VALUES (3, 2, 1, '2 1');

INSERT INTO yetanother VALUES (1, 1, '1 1 1');
INSERT INTO yetanother VALUES (2, 1, '1 1 2');
INSERT INTO yetanother VALUES (3, 2, '1 2 1');
