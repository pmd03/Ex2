-- Table: public.postlink

-- DROP TABLE public.postlink;

CREATE TABLE public.postlink
(
    "_CreationDate" timestamp without time zone,
    "_Id" integer,
    "_LinkTypeId" integer,
    "_PostId" integer,
    "_RelatedPostId" integer
)
TABLESPACE pg_default;

ALTER TABLE public.postlink
    OWNER to postgres;