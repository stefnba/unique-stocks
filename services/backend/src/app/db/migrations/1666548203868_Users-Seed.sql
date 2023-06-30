/* UP */
INSERT INTO users ("username", "email")
    VALUES ('John', 'john@gmail.com'), ('Paul', 'paul@paul.com');


/* DOWN */
TRUNCATE TABLE users CASCADE RESTART IDENTITY;

