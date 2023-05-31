--------------- WARNING: This is a periodic backup dump of the DB schema.
--------- For the most up-to-date schema, always prefer to query the DB itself,
--------- and refer to this schema only if the DB is unrecoverable.
--
-- PostgreSQL database dump
--

-- Dumped from database version 14.4 (Ubuntu 14.4-1.pgdg20.04+1)
-- Dumped by pg_dump version 14.4 (Ubuntu 14.4-1.pgdg20.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: postgis; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;


--
-- Name: EXTENSION postgis; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION postgis IS 'PostGIS geometry, geography, and raster spatial types and functions';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: level_2b; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.level_2b (
--  General data --
    granule_name text,
    shot_number bigint,
    beam_type text,
    beam_name text,
    delta_time double precision,
    absolute_time timestamp with time zone,
--  Quality data --
    algorithm_run_flag smallint,
    l2a_quality_flag smallint,
    l2b_quality_flag smallint,
    sensitivity real,
    degrade_flag smallint,
    stale_return_flag smallint,
    surface_flag smallint,
    solar_elevation real,
    solar_azimuth real,
--  Scientific data --
    cover real,
    cover_z real[],
    fhd_normal real,
    num_detectedmodes smallint,
    omega real,
    pai real,
    pai_z real[],
    pavd_z real[],
    pgap_theta real,
    pgap_theta_error real,
    pgap_theta_z real[],
    rg real,
    rh100 integer,
    rhog real,
    rhog_error real,
    rhov real,
    rhov_error real,
    rossg real,
    rv real,
    rx_range_highestreturn double precision,
--  DEM --
    dem_tandemx real,
--  Land cover data --
    gridded_leaf_off_flag smallint,
    gridded_leaf_on_doy int,
    gridded_leaf_on_cycle smallint,
    interpolated_modis_nonvegetated double precision,
    interpolated_modis_treecover double precision,
    gridded_pft_class smallint,
    gridded_region_class smallint,
--  Processing data --
    selected_l2a_algorithm smallint,
    selected_rg_algorithm smallint,
--  Geolocation data --
    lon_highestreturn double precision,
    lon_lowestmode double precision,
    longitude_bin0 double precision,
    longitude_bin0_error real,
    lat_highestreturn double precision,
    lat_lowestmode double precision,
    latitude_bin0 double precision,
    latitude_bin0_error real,
    elev_highestreturn double precision,
    elev_lowestmode double precision,
    elevation_bin0 double precision,
    elevation_bin0_error real,
    geometry public.geometry(Point,4326),
-- Waveform data --
    waveform_count bigint,
    waveform_start bigint
);


ALTER TABLE public.level_2b OWNER TO postgres;

--
-- Name: level_2b_granules; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.level_2b_granules (
    granule_name text NOT NULL,
    created_date timestamp with time zone
);


ALTER TABLE public.level_2b_granules OWNER TO postgres;


--
-- Name: level_4a; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.level_4a (
    granule_name text,
    shot_number bigint,
    beam_type text,
    beam_name text,
    delta_time double precision,
    absolute_time timestamp with time zone,
    sensitivity real,
    algorithm_run_flag smallint,
    degrade_flag smallint,
    l2_quality_flag smallint,
    l4_quality_flag smallint,
    predictor_limit_flag smallint,
    response_limit_flag smallint,
    surface_flag smallint,
    selected_algorithm smallint,
    selected_mode smallint,
    elev_lowestmode double precision,
    lat_lowestmode double precision,
    lon_lowestmode double precision,
    agbd double precision,
    agbd_pi_lower double precision,
    agbd_pi_upper double precision,
    agbd_se double precision,
    agbd_t double precision,
    agbd_t_se double precision,
    pft_class smallint,
    region_class smallint,
    leaf_off_flag smallint,
    geometry public.geometry(Point,4326)
);


ALTER TABLE public.level_4a OWNER TO postgres;

--
-- Name: level_4a_granules; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.level_4a_granules (
    granule_name text NOT NULL,
    created_date timestamp with time zone
);


ALTER TABLE public.level_4a_granules OWNER TO postgres;

--
-- Name: redd_boxes; Type: TABLE; Schema: public; Owner: sherwood
--

CREATE TABLE public.redd_boxes (
    geom_4326 public.geometry
);


ALTER TABLE public.redd_boxes OWNER TO sherwood;

ALTER TABLE ONLY public.level_2b_granules
    ADD CONSTRAINT level_2b_granules_pkey PRIMARY KEY (granule_name);


CREATE INDEX idx_level_2b_geom ON public.level_2b USING gist (geometry);
--
-- Name: level_4a_granules level_4a_granules_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.level_4a_granules
    ADD CONSTRAINT level_4a_granules_pkey PRIMARY KEY (granule_name);


--
-- Name: idx_level_4a_geom; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_level_4a_geom ON public.level_4a USING gist (geometry);


--
-- PostgreSQL database dump complete
--

