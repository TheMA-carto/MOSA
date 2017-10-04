-- /SCRIPT s'automatisation de la création de la BD MOSA
-- /Damien ROY aidé de Gilles Vuidel
-- /Laboratoire ThéMA Besancon
-- /Version du 05/06/2015 (Finale)



--
--/PARAMETRES
--

-- Explication :
--1 (Si besoin) Création du shéma dans lequel vont s'enregistrer les tables 
--2 Enregistrement (dans l'ordre: où vont s'effectuer les traitement, où chercher les données de bases, où chercher les données de bases complémantaires (si besoin), schéma basique (si besoin))
--3 Paramètre généraux (précision prec, taille de la grille gridsize, parametre de fonction de correction approx et snap, nombre de couche en oignon n_oignon, taille du buffer autour de la zone d'étude(uniquement pour les traitements)
		-- nature_troncon_route --
			-- Concerve les entités de routes dont le code associé 
			-- à la nature est inférieur ou égual à la valeur renseignée
			-- 1 'Autoroute', 2 'Quasi-autoroute', 3 'Bretelle', 4 'Route à 2 chaussées',
			-- 5 'Route à 1 chaussée', 6 'Route empierrée', 7 'Chemin', 
			-- 8 'Piste cyclable', 9 'Sentier', 10 'Escalier'
			-- Voir descriptif BD TOPO pour plus de détails
			
		-- nature_tvf --
			-- Concerve les entités de voies ferrées dont le code associé 
			-- à la nature est inférieur ou égual à la valeur renseignée
			-- 1 'LGV', 2 'Principale', 3 'Voie de service', 4 'Voie non exploitée', 
			-- 5 'Transport urbain', 6 'Funiculaire ou crémaillère' 
			-- Voir descriptif BD TOPO pour plus de détails	
--4 zone d'étude /!\ la table crée doit avoir pour nom 'zone'
--5 parametrage du traitement du RPG 1= champs cultivé, 2= prairie, 3= vergers, 4= autre
-- et paramétrage de corine land cover ('pp')

--/Modification du script pour paramétrage uniquement ici (et seulement entre les deux --/!\)
--/!\

		BEGIN;
	--1 
	DROP SCHEMA IF EXISTS mos_fp CASCADE;
	CREATE SCHEMA mos_fp;
	
	--2 
	SET search_path TO mos_fp,bdtopo_2010,public;

	--3 
	DROP VIEW IF EXISTS param;
	CREATE OR REPLACE VIEW param AS 
 	SELECT 
	4.9::double precision prec, --Largeur Minimal de Précision 
	250 gridsize, --taille du quadrillage (pour optimiser le traitement) 
	0.0001 approx, -- paramètre de gestion des erreurs (ne pas modifier)
	0.01 snap, -- paramètre de gestion des erreurs (ne pas modifier)
	5 n_oignon, -- nombre d'oignons du lors du traitement du RPG
	200 b_zone, -- Taille du Buffer autours de la zone pour éviter l'effet de bords
	10 nature_troncon_route, -- Voir explication ci-dessus
	6 nature_tvf, -- Voir explication ci-dessus
	2000 t_max_clairiere; --UMC des clairières
	-- ,'foret'::text choix_l
	-- ,'foret'::text choix_s;	
	COMMIT;		
	--4
	BEGIN;
	
	DROP TABLE IF EXISTS zone_buf;
	CREATE TABLE zone_buf AS
	SELECT st_buffer(st_union(zone.geom), b_zone) geom 
	FROM commune zone, param
	WHERE zone.nom LIKE 'Myon'
	GROUP BY b_zone;

	COMMIT;

	--5
	DROP TABLE IF EXISTS clc_mos; 
	CREATE TABLE clc_mos AS
	SELECT clc.* FROM clc_2006 clc, zone_buf, param
	WHERE st_intersects(clc.geom, zone_buf.geom);
	
	DROP TABLE IF EXISTS rpg_mos;
	CREATE TABLE rpg_mos AS
	SELECT rpg.* FROM rpg_2010_clean rpg, zone_buf, param
	WHERE st_intersects(rpg.geom, zone_buf.geom);
	
	DROP TABLE IF EXISTS param_rpg;
	CREATE TABLE param_rpg AS
	SELECT CASE 
		WHEN  cult_maj <= 10 OR cult_maj = 14 OR cult_maj = 15 
			OR cult_maj >= 24 AND cult_maj <= 26 
			THEN 1
	        WHEN cult_maj = 11 OR cult_maj >= 16 AND cult_maj <= 19 
			THEN 2
	        WHEN cult_maj = 27 OR cult_maj >= 20 AND cult_maj <= 23 
			THEN 3
	        ELSE 4
	          END AS typ, cult_maj FROM
		(SELECT DISTINCT cult_maj FROM rpg_mos
		ORDER BY cult_maj)t1;


--/!\

--
--/FONCTIONS AJOUTEES
--	
	BEGIN;
	--Fonction message + temps
	CREATE OR REPLACE FUNCTION msg(message text) 
	RETURNS void AS  
	$$
	BEGIN
		raise notice '%', message;
		raise notice '%', timeofday();
	END;
	$$ language plpgsql;
	
	--Fonction de nettoyage (retrace les polygones)
	CREATE OR REPLACE FUNCTION st_clean_poly(geom geometry, prec double precision)
	RETURNS geometry AS 
	$$
	DECLARE
		x integer;
		y integer;
		g geometry;
	BEGIN
		RETURN st_collectionExtract(st_makevalid(st_snaptogrid(
		st_collectionExtract(geom, 3), prec)), 3);
		EXCEPTION WHEN OTHERS THEN
		BEGIN
			RAISE NOTICE  'st_clean_poly : 
			Topology warning try increase precision by translating';
			x = st_minx(geom)::integer;
			y = st_miny(geom)::integer;
			g = st_translate(geom, -x, -y);
			g = st_collectionExtract(st_makevalid(
			st_snaptogrid(st_collectionExtract(g, 3), prec)), 3);
			RETURN st_translate(g, x, y);
			EXCEPTION WHEN OTHERS THEN
			BEGIN
				RAISE NOTICE  'st_clean_poly : Topology warning try buffer';
				RETURN st_collectionExtract(st_makevalid(st_snaptogrid(
				ST_Buffer(st_collectionExtract(geom, 3), prec/10.0), prec)), 3);
				EXCEPTION WHEN OTHERS THEN
					RAISE EXCEPTION  'st_clean_poly : 
					Topology error after buffer';
			END;
		END;
	END
	$$ LANGUAGE 'plpgsql' STABLE STRICT;

	--Fonction de nettoyage (extraire les polygones uniquement)
	CREATE OR REPLACE FUNCTION st_makevalid_poly(geom geometry)
	RETURNS geometry AS
	$$
	BEGIN
		RETURN st_collectionExtract(st_makevalid(st_collectionExtract(geom, 3)), 3);
	END
	$$
	LANGUAGE 'plpgsql' STABLE STRICT;

	--Fonction d'intersection avec nettoyage
	CREATE OR REPLACE FUNCTION st_inter_approx(geom_a geometry, geom_b geometry)
	RETURNS geometry AS 
	$$
	DECLARE
		x integer;
		y integer;
		g geometry;
	BEGIN
		RETURN ST_Intersection(geom_a, geom_b);
		EXCEPTION WHEN OTHERS THEN
		BEGIN
			RAISE NOTICE 'st_inter_approx : 
			Topology warning try increase precision by translating';
			x = least(st_minx(geom_a), st_minx(geom_b))::integer;
			y = least(st_miny(geom_a), st_miny(geom_b))::integer;
			g = st_intersection(st_translate(geom_a, -x, -y), 
			st_translate(geom_b, -x, -y));
			RETURN st_translate(g, x, y);
			EXCEPTION WHEN OTHERS THEN
			BEGIN
				RAISE NOTICE '
				st_inter_approx : Topology warning try buffer on 1';
				RETURN ST_Intersection(ST_Buffer(geom_a, 0.0001), geom_b);
				EXCEPTION WHEN OTHERS THEN
				BEGIN
					RAISE NOTICE '
					st_inter_approx : Topology warning try buffer on 2';
					RETURN ST_Intersection(geom_a, ST_Buffer(geom_b, 0.0001));
					EXCEPTION WHEN OTHERS THEN
					BEGIN
						RAISE NOTICE  '
						st_inter_approx : Topology warning try buffer on 1&2';
						RETURN ST_Intersection(ST_Buffer(geom_a, 0.0001),
						ST_Buffer(geom_b, 0.0001));
						EXCEPTION WHEN OTHERS THEN
						RAISE EXCEPTION  'st_inter_approx : 
						Topology error between % and %', 
						st_astext(geom_a), st_astext(geom_b);
					END;
				END;
			END;
		END;
	END
	$$
	LANGUAGE 'plpgsql' STABLE STRICT;

	--Fonction d'union avec nettoyage
	CREATE OR REPLACE FUNCTION st_union_approx(geom_a geometry, geom_b geometry)
	RETURNS geometry AS 
	$$
	DECLARE
		x integer;
		y integer;
		g geometry;
	BEGIN
		RETURN st_union(geom_a, geom_b);
		EXCEPTION WHEN OTHERS THEN
		BEGIN
			RAISE NOTICE 'st_union_approx : 
			Topology warning try increase precision by translating';
			x = least(st_minx(geom_a), st_minx(geom_b))::integer;
			y = least(st_miny(geom_a), st_miny(geom_b))::integer;
			g = st_union(st_translate(geom_a, -x, -y), st_translate(geom_b, -x, -y));
			RETURN st_translate(g, x, y);
			EXCEPTION WHEN OTHERS THEN
			BEGIN
				RAISE NOTICE '
				st_union_approx : Topology warning try buffer on 1';
				RETURN st_union(ST_Buffer(geom_a, 0.0001), geom_b);
				EXCEPTION WHEN OTHERS THEN
				BEGIN
					RAISE NOTICE  '
					st_union_approx : Topology warning try buffer on 2';
					RETURN st_union(geom_a, ST_Buffer(geom_b, 0.0001));
					EXCEPTION WHEN OTHERS THEN
					BEGIN
						RAISE NOTICE  'st_union_approx : 
						Topology warning try buffer on 1&2';
						RETURN st_union(ST_Buffer(geom_a, 0.0001), 
						ST_Buffer(geom_b, 0.0001));
						EXCEPTION WHEN OTHERS THEN
						RAISE EXCEPTION  'st_union_approx : 
						Topology error between % and %', 
						st_astext(geom_a), st_astext(geom_b);
					END;
				END;
			END;
		END;
	END
	$$
	LANGUAGE 'plpgsql' STABLE STRICT;

	--Fonction différence avec nettoyage
	CREATE OR REPLACE FUNCTION st_diff_approx(geom_a geometry, geom_b geometry)
	RETURNS geometry AS 
	$$
	DECLARE
		x integer;
		y integer;
		g geometry;
	BEGIN
		RETURN st_difference(geom_a, geom_b);
		EXCEPTION WHEN OTHERS THEN
		BEGIN
			RAISE NOTICE 'st_diff_approx : 
			Topology warning try increase precision by translating';
			x = least(st_minx(geom_a), st_minx(geom_b))::integer;
			y = least(st_miny(geom_a), st_miny(geom_b))::integer;
			g = st_difference(st_translate(geom_a, -x, -y), st_translate(geom_b, -x, -y));
			RETURN st_translate(g, x, y);
			EXCEPTION WHEN OTHERS THEN
			BEGIN
				RAISE NOTICE  'st_diff_approx : Topology warning try buffer on 2';
				RETURN ST_difference(geom_a, ST_Buffer(geom_b, 0.0001));
				EXCEPTION WHEN OTHERS THEN
				BEGIN
					RAISE NOTICE  'st_diff_approx : 
					Topology warning try buffer on 1&2';
					RETURN ST_difference(ST_Buffer(geom_a, 0.0001), 
					ST_Buffer(geom_b, 0.0001));
					EXCEPTION WHEN OTHERS THEN
					RAISE EXCEPTION  'st_diff_approx : 
					Topology error between % and %', 
					st_astext(geom_a), st_astext(geom_b);
				END;
			END;
		END;
	END
	$$
	LANGUAGE 'plpgsql' STABLE STRICT;

	--Fonction union avec nettoyage
	CREATE OR REPLACE FUNCTION st_union_approx(geom geometry)
	RETURNS geometry AS 
	$$
	DECLARE
		x integer;
		y integer;
		g geometry;
	BEGIN
		RETURN st_unaryunion(geom);
		EXCEPTION WHEN OTHERS THEN
		BEGIN
			RAISE NOTICE 'st_union_approx : 
			Topology warning try increase precision by translating';
			x = st_minx(geom)::integer;
			y = st_miny(geom)::integer;
			g = st_unaryunion(st_translate(geom, -x, -y));
			RETURN st_translate(g, x, y);
			EXCEPTION WHEN OTHERS THEN
			BEGIN
				RAISE NOTICE  'st_union_approx : Topology warning try buffer';
				RETURN ST_unaryunion(ST_Buffer(geom, 0.0001));
				EXCEPTION WHEN OTHERS THEN
				BEGIN
					RAISE NOTICE  'st_union_approx : 
					Topology warning try buffer*2';
					RETURN ST_unaryunion(ST_Buffer(geom, 0.0001*2));
					EXCEPTION WHEN OTHERS THEN
					RAISE EXCEPTION  'st_union_approx : 
					Topology error after buffer';
				END;
			END;
		END;
	END
	$$
	LANGUAGE 'plpgsql' STABLE STRICT;

	--Fonction merge avec nettoyage
	CREATE OR REPLACE FUNCTION merge_big_reste() 
	RETURNS void AS 
	$$
	BEGIN
    		FOUND := TRUE;
    		WHILE FOUND LOOP
    		PERFORM msg('Boucle reste');
    			INSERT INTO big_reste (
				SELECT DISTINCT reste_grid.* 
				FROM reste_grid, big_reste
				WHERE st_intersects(reste_grid.geom, big_reste.geom)
    			);
    			DELETE FROM reste_grid 
			WHERE id 
			in (SELECT id FROM big_reste);
    		END LOOP;
	END
	$$
	LANGUAGE 'plpgsql' VOLATILE STRICT;


	--Fonction merge spécifique aux lacune divisées par le quadrillage 
	CREATE OR REPLACE FUNCTION merge_mos_reste() 
	RETURNS void AS 
	$$
	BEGIN
    		FOUND := TRUE;
   		WHILE FOUND LOOP
    		PERFORM msg('Boucle mos reste');
    			DROP TABLE IF EXISTS tmp;
    			CREATE TEMPORARY TABLE tmp 
			AS
				SELECT DISTINCT r.* 
				FROM reste_grid r, mos_int_grid mos
				WHERE st_intersects(r.geom, mos.geom) and r.type = mos.type;
    				INSERT INTO mos_int_grid (
					SELECT type, geom 
					FROM tmp
    				);
    				DELETE FROM reste_grid 
				WHERE id 
				in (SELECT id FROM tmp);
    		END LOOP;
	END
	$$
	LANGUAGE 'plpgsql' VOLATILE STRICT;
	
	-- Fonction prenant en compte le parametre nature_troncon_route pour l'intégrer dans le code
	CREATE OR REPLACE FUNCTION choix_route(nb INT) 
	RETURNS RECORD 
	AS 
	$$
	DECLARE 
	  ret RECORD;
	BEGIN
	  IF nb = 1 THEN
	      ret := ('Autoroute'::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, 
		NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT);
	  ELSIF nb = 2 THEN
	      ret := ('Autoroute'::TEXT,'Quasi-autoroute'::TEXT, NULL::TEXT, NULL::TEXT, 
		NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT);
	  ELSIF nb = 3 THEN
	      ret := ('Autoroute'::TEXT,'Quasi-autoroute'::TEXT, 'Bretelle'::TEXT, NULL::TEXT, 
		NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT);
	  ELSIF nb = 4 THEN
	      ret := ('Autoroute'::TEXT,'Quasi-autoroute'::TEXT, 'Bretelle'::TEXT, 
		'Route à 2 chaussées'::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, 
		NULL::TEXT, NULL::TEXT, NULL::TEXT);
	  ELSIF nb = 5 THEN
	      ret := ('Autoroute'::TEXT,'Quasi-autoroute'::TEXT, 'Bretelle'::TEXT, 
		'Route à 2 chaussées'::TEXT, 'Route à 1 chaussée'::TEXT, NULL::TEXT, 
		NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT);  
	  ELSIF nb = 6 THEN
	      ret := ('Autoroute'::TEXT,'Quasi-autoroute'::TEXT, 'Bretelle'::TEXT, 
		'Route à 2 chaussées'::TEXT, 'Route à 1 chaussée'::TEXT, 'Route empierrée'::TEXT,
		 NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT); 
	   ELSIF nb = 7 THEN
	      ret := ('Autoroute'::TEXT,'Quasi-autoroute'::TEXT, 'Bretelle'::TEXT, 
		'Route à 2 chaussées'::TEXT, 'Route à 1 chaussée'::TEXT, 'Route empierrée'::TEXT,
		'Chemin'::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT); 
	   ELSIF nb = 8 THEN
	      ret := ('Autoroute'::TEXT,'Quasi-autoroute'::TEXT, 'Bretelle'::TEXT, 
		'Route à 2 chaussées'::TEXT, 'Route à 1 chaussée'::TEXT, 'Route empierrée'::TEXT,
		'Chemin'::TEXT, 'Piste cyclable'::TEXT, NULL::TEXT, NULL::TEXT); 
	   ELSIF nb = 9 THEN
	      ret := ('Autoroute'::TEXT,'Quasi-autoroute'::TEXT, 'Bretelle'::TEXT, 
		'Route à 2 chaussées'::TEXT, 'Route à 1 chaussée'::TEXT, 'Route empierrée'::TEXT,
		'Chemin'::TEXT, 'Piste cyclable'::TEXT, 'Sentier'::TEXT, NULL::TEXT);
	   ELSE
	      ret := ('Autoroute'::TEXT,'Quasi-autoroute'::TEXT, 'Bretelle'::TEXT, 
		'Route à 2 chaussées'::TEXT, 'Route à 1 chaussée'::TEXT, 'Route empierrée'::TEXT,
		'Chemin'::TEXT, 'Piste cyclable'::TEXT, 'Sentier'::TEXT, 'Escalier'::TEXT);  
	   END IF;
	RETURN ret;
	END;
	$$ LANGUAGE plpgsql;
	
	
	-- Fonction prenant en compte le parametre choix_vf pour l'intégrer dans le code
	CREATE OR REPLACE FUNCTION choix_vf(nb INT) 
	RETURNS RECORD 
	AS 
	$$
	DECLARE 
	  ret RECORD;
	BEGIN
	  IF nb = 1 THEN
	      ret := ('LGV'::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT);
	  ELSIF nb = 2 THEN
	      ret := ('LGV'::TEXT, 'Principale'::TEXT, NULL::TEXT, 
		NULL::TEXT, NULL::TEXT, NULL::TEXT);
	  ELSIF nb = 3 THEN
	      ret := ('LGV'::TEXT, 'Principale'::TEXT, 'Voie de service'::TEXT, 
		NULL::TEXT, NULL::TEXT, NULL::TEXT);
	  ELSIF nb = 4 THEN
	      ret := ('LGV'::TEXT, 'Principale'::TEXT, 'Voie de service'::TEXT, 
		'Voie non exploitée'::TEXT, NULL::TEXT, NULL::TEXT);
	  ELSIF nb = 5 THEN
	      ret := ('LGV'::TEXT, 'Principale'::TEXT, 'Voie de service'::TEXT, 
		'Voie non exploitée'::TEXT, 'Transport urbain'::TEXT, NULL::TEXT);
	   ELSE
	      ret := ('LGV'::TEXT, 'Principale'::TEXT, 'Voie de service'::TEXT, 
		'Voie non exploitée'::TEXT, 'Transport urbain'::TEXT, 
		'Funiculaire ou crémaillère'::TEXT);
	   END IF;
	RETURN ret;
	END;
	$$ LANGUAGE plpgsql;

	--Fonction maximum
	CREATE OR REPLACE FUNCTION max(val1 bigint, val2 bigint)
		RETURNS bigint AS  
		$$
		BEGIN
			IF val1>=val2 THEN return val1;
			ELSE return val2;
			END IF;
		END
		$$ language plpgsql;
	COMMIT;
	
	
--
--/SCRIPT PARTIE 1 PREPARATION
--

	BEGIN;
	-- tracer la grille
	DROP TABLE IF EXISTS grid;
	CREATE TABLE grid 
	AS
		SELECT row_number() over () id, g.geom
		FROM (
			SELECT ST_Translate(st_scale(cell, gridsize, gridsize), i * gridsize + x1
			, j * gridsize + y1) AS geom
			FROM param,
				(
				SELECT ('SRID=2154;POLYGON((0 0, 0 1, 1 1, 1 0,0 0))')::geometry 
				AS cell
				) t,
				(
				SELECT st_xmin(geom)::integer x1, st_ymin(geom)::integer y1 
				FROM zone_buf
				) extent,
				generate_series(0, (SELECT ceil(st_xmax(geom))::integer-
				st_xmin(geom)::integer FROM zone_buf)/(SELECT gridsize FROM param)) 
				AS i, 
				generate_series(0, (SELECT ceil(st_ymax(geom))::integer-
				st_ymin(geom)::integer FROM zone_buf)/(SELECT gridsize FROM param)) 
				AS j 
				) g, 
				zone_buf z;

	ALTER TABLE grid ADD CONSTRAINT grid_pk PRIMARY KEY(id);
	CREATE INDEX sidx_grid_geom ON grid USING gist (geom);
	ANALYZE grid; 

	-- Retire de la grille les CASEs ne se superposant pas à la zone d'étude
	DELETE FROM grid
	WHERE grid.id IN (
		SELECT grid.id FROM grid, zone_buf WHERE NOT st_intersects(grid.geom, zone_buf.geom)
	);
	ANALYZE grid; 
	SELECT msg('END Partie 1 PREPARATION');
	COMMIT;

--
--/SCRIPT PARTIE 2 EAU 
--

	BEGIN;
	--Utilisation de troncon court d'eau pour retirer les surfaces en eau sousterraines 
	--et utilisation de l'attribut regime pour retirer les eaux temporaires
	SELECT msg('Start Partie 2 EAU');
	DROP TABLE IF EXISTS eau_select;
	CREATE TEMPORARY TABLE eau_select
	AS 
		SELECT eau.geom
		FROM zone_buf z, param,
		(
			(
				SELECT ea.geom 
				FROM surface_eau ea, zone_buf z
				WHERE st_intersects(z.geom, ea.geom) 
				AND ea.regime::text = 'Permanent'::text
			) tt1
			LEFT JOIN
			(
				SELECT st_centroid(tce.geom) geom_centro 
				FROM troncon_cours_eau tce, zone_buf z 
				WHERE pos_sol = -1 AND st_intersects(z.geom, tce.geom)
			)tt2
			on st_intersects(tt1.geom, tt2.geom_centro)
		) eau
		WHERE eau.geom_centro IS NULL;
	CREATE INDEX sidx_eau_select_geom ON eau_select USING gist (geom);

	--Création de la surcouche iles
	SELECT msg('Sous Partie 2.1 EAU');
	DROP TABLE IF EXISTS iles;
	CREATE TABLE iles
	AS 
		SELECT tte.geom AS geom, st_area(tte.geom) AS area FROM (
		SELECT (st_dump(ST_difference(st_buildarea(st_exteriorring(e.geom)),e.geom))).geom AS geom
		FROM 
			(
			SELECT (st_dump(st_union(eau.geom))).geom
			FROM eau_select eau
			) e
		) tte;
	CREATE INDEX sidx_iles_geom ON iles USING gist (geom);
	ANALYZE iles; 
			
	--Comblement des iles
	SELECT msg('Sous Partie 2.2 EAU');
	DROP TABLE IF EXISTS eau_full;
	CREATE TEMPORARY TABLE eau_full
	AS
		SELECT st_buildarea(st_exteriorring(eau.geom)) AS geom
		FROM  
			(
			SELECT (st_dump(st_union(eau.geom))).geom AS geom 
			FROM eau_select eau
			) eau;
	CREATE INDEX sidx_eau_full_geom ON eau_full USING gist (geom);
	
	--Rétablissement des iles supérieures à l'UMC
	SELECT msg('Sous partie 2.3 EAU');
	DROP TABLE IF EXISTS eau_ok;
	CREATE Temporary TABLE eau_ok
	AS
		SELECT ST_Difference(eau_full.geom, st_union(
	    		CASE WHEN iles.geom IS null 
			THEN st_geomfromtext('GEOMETRYCOLLECTION EMPTY') 
			ELSE iles.geom END)) AS geom
		FROM 
		eau_full
		LEFT JOIN 
		( 
		SELECT iles.geom FROM iles WHERE iles.area > 2000
		) iles 
		on st_intersects(eau_full.geom, iles.geom) 
		GROUP BY eau_full.geom;
	CREATE INDEX sidx_eau_ok_geom ON eau_ok USING gist (geom);

	--Finalisation et validation des polygones d'eau
	SELECT msg('Sous partie 2.4 finition EAU');
	DROP TABLE IF EXISTS eau_int;
	CREATE TABLE eau_int
	AS
		SELECT st_makevalid(st_force2D((st_dump(st_union(eau_ok.geom))).geom)) AS geom
		FROM eau_ok;
	CREATE INDEX sidx_eau_int_geom ON eau_int USING gist (geom);
	
	--Attribution d'un identifant
	SELECT msg('Sous partie 2.4 finition EAU');
	DROP TABLE IF EXISTS eau;
	CREATE TABLE eau
	AS
		SELECT row_number() over () id, geom
		FROM eau_int;
	CREATE INDEX sidx_eau_geom ON eau USING gist (geom);
	ANALYZE eau; 
	COMMIT;

	--Surcouches optionnelles
	BEGIN;
	SELECT msg('Surcouche eau_temp  EAU');
	DROP TABLE IF EXISTS surc_eau_temp;
	CREATE TABLE surc_eau_temp AS
	SELECT ea.* FROM surface_eau ea, zone_buf z
	WHERE ea.regime::text <> 'Permanent'::text AND st_intersects(z.geom, ea.geom);
	CREATE INDEX sidx_surc_eau_temp_geom ON surc_eau_temp USING gist (geom);

	SELECT msg('Surcouche eau_small  EAU');
	DROP TABLE IF EXISTS surc_eau_small;
	CREATE TABLE surc_eau_small AS
	SELECT se.* FROM surface_eau se, zone_buf z
	WHERE st_area(se.geom) < 2000 AND st_intersects(z.geom, se.geom) ;
	CREATE INDEX sidx_surc_eau_small_geom ON surc_eau_small USING gist (geom);
	COMMIT;

--
--/SCRIPT PARTIE 3 ROUTE
--

	BEGIN;
	-- merge des routes et des voies ferrées bufferisées centre, gauche et droit
	-- gauche et droite de la route nécessaire pour la ligature des organes au squelette polygonal
	-- ainsi les organes à gauche du squelette serons ligaturés à gauche de celui-ci 
	-- et ne déborde pas à droite. Et vis versa.
	SELECT msg('Start Partie 3 ROUTE');
	SELECT msg('Sous_Partie 3.1 ROUTE');
	DROP TABLE IF EXISTS voie_buf_0;
	CREATE TABLE voie_buf_0
	AS 		
		-- traitement des routes( emprise réelle et emprise buffurisé gauche et droite)	
		-- Sélection des routes en fonction de l'attribut nature pour retirer les routes
		-- ne correspondant pas à une occupation du sol au sens défini pour le MOS (tunnel et bac)
		SELECT r.larg, r.geom, r.nature,
		st_buffer(r.geom, r.larg / 2::double precision) 
			AS geom_buf,
		st_buffer(
			st_offsetcurve(st_geometryn(r.geom, 1),(r.larg / 2::double precision +
			prec) / 2::double precision), 
			(r.larg / 2::double precision + prec) / 2::double precision) 
			AS geom_right, 
		st_buffer(
			st_offsetcurve(st_geometryn(r.geom, 1), (- (r.larg / 2::double precision +
			prec)) / 2::double precision), 
			(r.larg / 2::double precision + prec) / 2::double precision) 
			AS geom_left
		FROM ( 
			SELECT CASE 
			WHEN r.largeur <= p.prec
			THEN p.prec - 0.1
			ELSE r.largeur 
			END 
			AS larg,r.* 
        		FROM route r, zone_buf z, param p
        		WHERE st_intersects(z.geom, r.geom) 
			AND r.franchisst::text <> 'Tunnel'::text 
			AND r.nature::text <> 'Bac auto' 
			AND r.nature::text <> 'Bac piéton'
		) r, param;

	ALTER TABLE voie_buf_0 ALTER COLUMN nature TYPE varchar(30);
	
		-- traitement des VF( emprise réelle et emprise buffurisé gauche et droite)
		-- Traitement particulier de la LGV qui bénéficie d'une largeure de voies supérieure
	SELECT msg('Sous_Partie 3.2 VF');
	INSERT INTO voie_buf_0 (
		SELECT tvf.larg, tvf.geom, tvf.nature,
		st_buffer(tvf.geom, tvf.larg / 2::double precision) 
			AS geom_buf,
		st_buffer(
			st_offsetcurve(st_geometryn(tvf.geom, 1), ((tvf.larg / 2::double precision +
			prec) / 2)), 
			((tvf.larg / 2::double precision + prec) / 2)) 
			AS geom_right, 
		st_buffer(
			st_offsetcurve(st_geometryn(tvf.geom, 1), ((-(tvf.larg / 2::double precision +
			prec)) / 2)), 
			((tvf.larg / 2::double precision + prec) / 2)) 
			AS geom_left
		FROM ( 
			SELECT CASE 
				WHEN tvf.nature = 'LGV'::text 
				THEN tvf.nb_voies * 10::double precision 
				ELSE tvf.nb_voies * 5
				END 
			AS larg, tvf.geom, tvf.nature
        		FROM troncon_voie_ferree tvf, zone_buf z
        		WHERE st_intersects(z.geom, tvf.geom) 
			AND tvf.franchisst::text <> 'Tunnel'::text
		) tvf, param);

	--Ajout des surfaces de route
	SELECT msg('Sous_Partie 3.3 SURFACE ROUTE');			
	INSERT INTO voie_buf_0 ( 				--
		SELECT 999 AS larg, st_multi(st_ExteriorRing((st_dump(sr.geom)).geom)) AS geom, 
		sr.nature AS nature, 				--
			sr.geom AS geom_buf,			--
		st_buffer(sr.geom, p.prec) AS geom_right,
		st_geomfromtext('GEOMETRYCOLLECTION EMPTY') AS geom_left
        	FROM surface_route sr, zone_buf z, param p
        	WHERE st_intersects(z.geom, sr.geom)); 

	--Ajout des aires de triage
	INSERT INTO voie_buf_0 ( 				--
		SELECT 888 AS larg, st_multi(st_ExteriorRing((st_dump(sr.geom)).geom)) AS geom, 
		'air_t' AS nature, 				--
			sr.geom AS geom_buf,			--
		st_buffer(sr.geom, p.prec) AS geom_right,
		st_geomfromtext('GEOMETRYCOLLECTION EMPTY') AS geom_left
        	FROM aire_triage sr, zone_buf z, param p
        	WHERE st_intersects(z.geom, sr.geom)); 
			
	CREATE INDEX sidx_voie_buf_0_geom_buf ON voie_buf_0 USING gist (geom_buf);
	CREATE INDEX sidx_voie_buf_0_geom_left ON voie_buf_0 USING gist (geom_left);
	CREATE INDEX sidx_voie_buf_0_geom_right ON voie_buf_0 USING gist (geom_right);
	ANALYZE voie_buf_0;

	--Attribution d'un identifant
	DROP TABLE IF EXISTS voie_buf_1;
	CREATE TABLE voie_buf_1
	AS 
		SELECT row_number() over () id, vb0.larg, vb0.geom, vb0.nature, vb0.geom_buf,
		vb0.geom_right, vb0.geom_left
		FROM voie_buf_0 vb0;
	
	--Finalisation et validation des polygones de route
	SELECT msg('Sous_Partie 3.tris ROUTE');	
	DROP TABLE IF EXISTS voie_buf;
	CREATE TABLE voie_buf
	AS 	
		SELECT id, larg, geom, nature, st_force2D(geom_buf) AS geom_buf,
		st_force2D(geom_right) AS geom_right, st_force2D(geom_left) AS geom_left
		FROM voie_buf_1;		--
	CREATE INDEX sidx_voie_buf_geom_buf ON voie_buf USING gist (geom_buf);
	CREATE INDEX sidx_voie_buf_geom_left ON voie_buf USING gist (geom_left);
	CREATE INDEX sidx_voie_buf_geom_right ON voie_buf USING gist (geom_right);
	COMMIT;


	--Couche concervé dans le MOS final après application des paramètres
	BEGIN;
	SELECT msg('Sous_Partie 3.4 SELECT ROUTE');
	DROP TABLE IF EXISTS route_select;
	CREATE TABLE route_select
	AS 	
		SELECT vb. geom, vb.geom_buf AS geom_buf_s, vb.id AS id2, vb.nature
		FROM voie_buf_1 vb			
		WHERE vb.nature IN ( 
			(SELECT a FROM (
				SELECT a, b, c, d, e, f, g, h, i, j FROM choix_route(
					(SELECT nature_troncon_route FROM param)
				) AS (a text, b TEXT, c TEXT, d text, e TEXT, 
				f TEXT, g text, h TEXT, i TEXT, j TEXT))t1), 
			(SELECT b FROM (
				SELECT a, b, c, d, e, f, g, h, i, j FROM choix_route(
					(SELECT nature_troncon_route FROM param)
				) AS (a text, b TEXT, c TEXT, d text, e TEXT,
				 f TEXT, g text, h TEXT, i TEXT, j TEXT))t1),
			(SELECT c FROM (
				SELECT a, b, c, d, e, f, g, h, i, j FROM choix_route(
					(SELECT nature_troncon_route FROM param)
				) AS (a text, b TEXT, c TEXT, d text, e TEXT,
				 f TEXT, g text, h TEXT, i TEXT, j TEXT))t1),
			(SELECT d FROM (
				SELECT a, b, c, d, e, f, g, h, i, j FROM choix_route(
					(SELECT nature_troncon_route FROM param)
				) AS (a text, b TEXT, c TEXT, d text, e TEXT,
				 f TEXT, g text, h TEXT, i TEXT, j TEXT))t1),
			(SELECT e FROM (
				SELECT a, b, c, d, e, f, g, h, i, j FROM choix_route(
					(SELECT nature_troncon_route FROM param)
				) AS (a text, b TEXT, c TEXT, d text, e TEXT,
				 f TEXT, g text, h TEXT, i TEXT, j TEXT))t1),
			(SELECT f FROM (
				SELECT a, b, c, d, e, f, g, h, i, j FROM choix_route(
					(SELECT nature_troncon_route FROM param)
				) AS (a text, b TEXT, c TEXT, d text, e TEXT,
				 f TEXT, g text, h TEXT, i TEXT, j TEXT))t1),
			(SELECT g FROM (
				SELECT a, b, c, d, e, f, g, h, i, j FROM choix_route(
					(SELECT nature_troncon_route FROM param)
				) AS (a text, b TEXT, c TEXT, d text, e TEXT,
				 f TEXT, g text, h TEXT, i TEXT, j TEXT))t1),
			(SELECT h FROM (
				SELECT a, b, c, d, e, f, g, h, i, j FROM choix_route(
					(SELECT nature_troncon_route FROM param)
				) AS (a text, b TEXT, c TEXT, d text, e TEXT,
				 f TEXT, g text, h TEXT, i TEXT, j TEXT))t1),
			(SELECT i FROM (
				SELECT a, b, c, d, e, f, g, h, i, j FROM choix_route(
					(SELECT nature_troncon_route FROM param)
				) AS (a text, b TEXT, c TEXT, d text, e TEXT,
				 f TEXT, g text, h TEXT, i TEXT, j TEXT))t1),
			(SELECT j FROM (
				SELECT a, b, c, d, e, f, g, h, i, j FROM choix_route(
					(SELECT nature_troncon_route FROM param)
				) AS (a text, b TEXT, c TEXT, d text, e TEXT,
				 f TEXT, g text, h TEXT, i TEXT, j TEXT))t1)
		);
	CREATE INDEX sidx_route_select_geom_buf_s ON route_select USING gist (geom_buf_s);

	--Couche concervé dans le MOS final après application des paramètres
	SELECT msg('Sous_Partie 3.4 SELECT Surf_ROUTE');
	DROP TABLE IF EXISTS surf_route_select;		
	CREATE TABLE surf_route_select			
	AS 
		SELECT row_number() over () id, sr.geom, sr.nature
        	FROM surface_route sr, zone_buf z
        	WHERE st_intersects(z.geom, sr.geom);
	CREATE INDEX sidx_surf_route_select_geom ON surf_route_select USING gist (geom);

	--Couche concervé dans le MOS final après application des paramètres
	SELECT msg('Sous_Partie 3.4 SELECT VF');
	DROP TABLE IF EXISTS vf_select;	
	CREATE TABLE vf_select
	AS 
		SELECT vb. geom, vb.geom_buf AS geom_buf_s, vb.id AS id2, vb.nature
		FROM voie_buf_1 vb	
		WHERE vb.nature IN ( 
			(SELECT a FROM (
				SELECT a, b, c, d, e, f FROM choix_vf(
					(SELECT nature_tvf FROM param)
				) AS (a text, b TEXT, c TEXT, d text, e TEXT, f TEXT))t1), 
			(SELECT b FROM (
				SELECT a, b, c, d, e, f FROM choix_vf(
					(SELECT nature_tvf FROM param)
				) AS (a text, b TEXT, c TEXT, d text, e TEXT, f TEXT))t1), 
			(SELECT c FROM (
				SELECT a, b, c, d, e, f FROM choix_vf(
					(SELECT nature_tvf FROM param)
				) AS (a text, b TEXT, c TEXT, d text, e TEXT, f TEXT))t1), 
			(SELECT d FROM (
				SELECT a, b, c, d, e, f FROM choix_vf(
					(SELECT nature_tvf FROM param)
				) AS (a text, b TEXT, c TEXT, d text, e TEXT, f TEXT))t1), 
			(SELECT e FROM (
				SELECT a, b, c, d, e, f FROM choix_vf(
					(SELECT nature_tvf FROM param)
				) AS (a text, b TEXT, c TEXT, d text, e TEXT, f TEXT))t1), 
			(SELECT f FROM (
				SELECT a, b, c, d, e, f FROM choix_vf(
					(SELECT nature_tvf FROM param)
				) AS (a text, b TEXT, c TEXT, d text, e TEXT, f TEXT))t1)
		);
	CREATE INDEX sidx_vf_select_geom_buf_s ON vf_select USING gist (geom_buf_s);

	--Couche concervé dans le MOS final après application des paramètres
	SELECT msg('Sous_Partie 3.4 SELECT tot');
	DROP TABLE IF EXISTS select_tot_vf_r_temp;	
	CREATE TEMPORARY TABLE select_tot_vf_r_temp	
	AS 
		SELECT id2, geom_buf_s FROM route_select;
	INSERT INTO select_tot_vf_r_temp (SELECT id2, geom_buf_s FROM vf_select);
	INSERT INTO select_tot_vf_r_temp (SELECT id AS id2, geom AS geom_buf_s FROM voie_buf_1 WHERE larg = 999);

	--Couche concervé dans le MOS final après application des paramètres
	SELECT msg('1');
	DROP TABLE IF EXISTS select_tot_vf_r;
	CREATE TABLE select_tot_vf_r
	AS 
		SELECT row_number() over () id, st_makevalid(st_force2d(st_buffer(geom_buf_s,0))) geom 
		FROM select_tot_vf_r_temp;
	ALTER TABLE select_tot_vf_r ADD CONSTRAINT select_tot_vf_r_pk PRIMARY KEY(id);
	CREATE INDEX sidx_select_tot_vf_r_geom ON select_tot_vf_r USING gist (geom);
	COMMIT;


	--Surcouches optionnelles
	--Tunnel
	BEGIN;
	SELECT msg('Surcouche tunnel  Route 1/3');
	DROP TABLE IF EXISTS surc_tunnel_0;
	CREATE TEMPORARY TABLE surc_tunnel_0 AS
	SELECT r.franchisst, r.geom, st_buffer(r.geom, r.larg/2)AS geom_buf FROM ( 
			SELECT CASE 
			WHEN r.largeur <= p.prec 
			THEN p.prec - 0.1
			ELSE r.largeur 
			END 
			AS larg, r.geom, r.franchisst
        		FROM route r, zone_buf z, param p
        		WHERE st_intersects(z.geom, r.geom) 
			AND r.franchisst::text = 'Tunnel'::text
		) r;

	SELECT msg('Surcouche tunnel  vf 2/3');
	INSERT INTO surc_tunnel_0 (
		SELECT tvf.franchisst, tvf.geom, st_buffer(tvf.geom, tvf.larg/2) AS geom_buf FROM(
			SELECT CASE 
				WHEN tvf.nature = 'LGV'::text 
				THEN tvf.nb_voies * 10::double precision 
				ELSE tvf.nb_voies * 5
				END 
			AS larg, tvf.geom, tvf.franchisst
        		FROM troncon_voie_ferree tvf, zone_buf z
        		WHERE st_intersects(z.geom, tvf.geom) 
			AND tvf.franchisst::text = 'Tunnel'::text
		) tvf);

	SELECT msg('Surcouche tunnel  Route-vf 3/3');
	DROP TABLE IF EXISTS surc_tunnel;
	CREATE TABLE surc_tunnel
	AS 
		SELECT row_number() over () id, st_0.franchisst, st_0.geom, st_0.geom_buf
		FROM surc_tunnel_0 st_0;
	CREATE INDEX sidx_surc_tunnel_geom ON surc_tunnel USING gist (geom);
	CREATE INDEX sidx_surc_tunnel_geom_buf ON surc_tunnel USING gist (geom_buf);
	ANALYZE surc_tunnel; 
	COMMIT;

	--Pont
	BEGIN;
	SELECT msg('Surcouche pont  Route 1/3');
	DROP TABLE IF EXISTS surc_pont_0;
	CREATE TEMPORARY TABLE surc_pont_0 AS
	SELECT r.franchisst, r.geom, st_buffer(r.geom, r.larg/2)AS geom_buf FROM ( 
			SELECT CASE 
			WHEN r.largeur <= p.prec 
			THEN p.prec - 0.1
			ELSE r.largeur 
			END 
			AS larg,r.* 
        		FROM route r, zone_buf z, param p
        		WHERE st_intersects(z.geom, r.geom) 
			AND r.franchisst::text <> 'Tunnel'::text
			AND r.franchisst::text <> 'NC'::text
		) r;
		
	SELECT msg('Surcouche pont  vf 2/3');
	INSERT INTO surc_pont_0 (
		SELECT tvf.franchisst, tvf.geom, st_buffer(tvf.geom, tvf.larg/2) AS geom_buf FROM(
			SELECT CASE 
				WHEN tvf.nature = 'LGV'::text 
				THEN tvf.nb_voies * 10::double precision 
				ELSE tvf.nb_voies * 5
				END 
			AS larg, tvf.geom, tvf.franchisst
        		FROM troncon_voie_ferree tvf, zone_buf z
        		WHERE st_intersects(z.geom, tvf.geom) 
			AND tvf.franchisst::text <> 'Tunnel'::text
			AND tvf.franchisst::text <> 'NC'::text
		) tvf);
	
	SELECT msg('Surcouche pont  Route-vf 3/3');
	DROP TABLE IF EXISTS surc_pont;
	CREATE TABLE surc_pont
	AS 
		SELECT row_number() over () id, sp_0.franchisst, sp_0.geom, sp_0.geom_buf
		FROM surc_pont_0 sp_0;
	CREATE INDEX sidx_surc_pont_geom ON surc_pont USING gist (geom);
	CREATE INDEX sidx_surc_pont_geom_buf ON surc_pont USING gist (geom_buf);
	ANALYZE surc_pont; 

	COMMIT;



--
--/SCRIPT PARTIE 4 RPG
--

	BEGIN;
	--/Cathégoriser et Combler vis à vis de la LMC 
	SELECT msg('start PARTIE 4 RPG');
	DROP TABLE IF EXISTS rpg_typ;
	CREATE TABLE rpg_typ AS 
	SELECT row_number() over () id, *
	FROM (
		-- comble les vides inférieur à la LMC intra-cathégorie, singlepart
		SELECT champs_type.typ, (st_dump(st_buffer(st_union(st_makevalid(
		st_buffer(champs_type.geom, prec / 2))), -prec / 2))).geom 
		AS geom
		FROM param,
		( 
				(
				SELECT rpg_clean.*
				FROM zone_buf, rpg_mos rpg_clean
				WHERE st_intersects(rpg_clean.geom, zone_buf.geom) 
				) t1
				LEFT JOIN
				param_rpg pr
				on ( t1.cult_maj= pr.cult_maj)
		) champs_type
		GROUP BY champs_type.typ, prec
	) t;
	ALTER TABLE rpg_typ ADD CONSTRAINT rpg_typ_pk PRIMARY KEY(id);
	CREATE INDEX sidx_rpg_typ_geom ON rpg_typ USING gist (geom);
	ANALYZE rpg_typ;
	SELECT msg('END sous-partie 4.1 RPG');
	COMMIT;

	BEGIN;
	--/Suppression des supperpositions entre route et champs 
	--/Permet de préparer les champs à l'étape suivante
	DROP TABLE IF EXISTS rpg_typ_ssvoie;
	CREATE TABLE rpg_typ_ssvoie 
	AS 
		SELECT row_number() over () id, *
		FROM ( 
			-- single part
			SELECT typ, (st_dump(geom)).geom geom 
			FROM (
				-- suppression des surfaces recouvrant des voies
				SELECT rpg.id, typ, st_difference(rpg.geom, st_union(
	    				CASE WHEN r.geom_buf IS null 
					THEN st_geomfromtext('GEOMETRYCOLLECTION EMPTY') 
					ELSE r.geom_buf END)) 
					AS geom
				FROM rpg_typ rpg 
				LEFT JOIN voie_buf r 
				ON st_intersects(rpg.geom, r.geom_buf)
				GROUP BY rpg.id, typ, rpg.geom
			) t1
		) t2
		WHERE st_area(geom)>500;

	ALTER TABLE rpg_typ_ssvoie ADD CONSTRAINT rpg_typ_ssvoie_pk PRIMARY KEY(id);
	CREATE INDEX sidx_rpg_typ_ssvoie_geom ON rpg_typ_ssvoie USING gist (geom);
	ANALYZE rpg_typ_ssvoie;
	SELECT msg('END sous-partie 4.2 RPG');
	COMMIT;

	
	BEGIN;
	--/Raccordement au squelette polygonal
	--/ rpg champs collé aux voies quand distance < prec

	SELECT msg('Start raccordement sous-partie 4.3 RPG');

	SELECT msg(' rpg_left ');
	DROP TABLE IF EXISTS rpg_left;
	CREATE TABLE rpg_left AS
				SELECT rpg.id, st_buffer(st_buffer(st_buffer(
				-- MODIF du 05/03 AJOUT st_buffer( ... , snap) pour regler des problemes récurents
				st_union_approx(st_collect(st_makevalid_poly(
				st_union(rpg.geom, int_buf.geom)
				))),
				snap), -prec/2), prec/2) geom
				FROM rpg_typ_ssvoie rpg, param,
				(
					SELECT rpg_r.id, st_difference(st_makevalid_poly( 
					(st_dump(st_intersection(st_buffer(
					rpg_r.geom, prec), r.geom_left)
					)).geom),r.geom_buf) geom
					FROM rpg_typ_ssvoie rpg_r, voie_buf r, param
					WHERE st_intersects(rpg_r.geom, r.geom_left)
				) int_buf
				WHERE int_buf.id=rpg.id 
				GROUP BY rpg.id, prec, snap;

	SELECT msg(' rpg_right ');
	DROP TABLE IF EXISTS rpg_right;
	CREATE TABLE rpg_right AS
				--Sous partie 4.3.1 (traite les éléments à droite de la route)
				SELECT rpg.id, st_buffer(st_buffer(st_buffer(
				-- MODIF du 05/03 AJOUT st_buffer( ... , snap) pour regler des problemes récurents
				st_union_approx(st_collect(st_makevalid_poly(
				st_union(rpg.geom, int_buf.geom)
				))), 
				snap), -prec/2), prec/2) geom
				FROM rpg_typ_ssvoie rpg, param,
				(
					SELECT rpg_r.id, st_difference(st_makevalid_poly(
					(st_dump(st_intersection(st_buffer(
					rpg_r.geom, prec), r.geom_right)
					)).geom),r.geom_buf) geom
					FROM rpg_typ_ssvoie rpg_r, voie_buf r, param
					WHERE st_intersects(rpg_r.geom, r.geom_right)
				) int_buf
				WHERE int_buf.id=rpg.id
				GROUP BY rpg.id, prec, snap;

	SELECT msg('rpg_lr');
	DROP TABLE IF EXISTS rpg_lr;
	CREATE TABLE rpg_lr AS
			--Sous partie 4.3.2 (traite les éléments à gauche de la route)
			SELECT CASE 
			WHEN rpg_left.id IS null 
			THEN rpg_right.id 
			ELSE rpg_left.id 
			END id, 
			st_union( CASE WHEN rpg_left.geom IS null 
				THEN st_geomfromtext('GEOMETRYCOLLECTION EMPTY') 
				ELSE rpg_left.geom 
				END, 
       				CASE WHEN rpg_right.geom IS null 
				THEN st_geomfromtext('GEOMETRYCOLLECTION EMPTY') 
				ELSE rpg_right.geom 
			END) geom
			FROM rpg_left 
			FULL JOIN
			rpg_right
			on (rpg_left.id = rpg_right.id);

	SELECT msg('rpg_left_right');
	DROP TABLE IF EXISTS rpg_left_right;
	CREATE TABLE rpg_left_right 
	AS
		--Sous partie 4.3.3 Fusionne le resultat de (4.3.1 4.3.2) avec le rpg (4.2)
		SELECT  rpg.id, rpg.typ, st_makevalid_poly(st_union_approx(rpg.geom, 
			CASE WHEN rpg_lr.geom IS null 
			THEN st_geomfromtext('GEOMETRYCOLLECTION EMPTY') 
			ELSE rpg_lr.geom END
			)) geom
		FROM rpg_typ_ssvoie rpg
		LEFT JOIN
		rpg_lr 
		on rpg.id = rpg_lr.id;

	ALTER TABLE rpg_left_right ADD CONSTRAINT rpg_left_right_pk PRIMARY KEY(id);
	CREATE INDEX sidx_rpg_left_right_geom ON rpg_left_right USING gist (geom);
	ANALYZE rpg_left_right;
	SELECT msg('END sous-partie 4.3 RPG');
	COMMIT;




	BEGIN;
	-- rpg_left_right sans les voies
	-- suppression des surfaces recouvrant des voies
	-- single part
	DROP TABLE IF EXISTS rpg_left_right_ssvoie;
	CREATE TEMPORARY TABLE rpg_left_right_ssvoie 
	AS 
		SELECT row_number() over () id, typ, geom
		FROM ( 
			SELECT typ, (st_dump(st_clean_poly(geom, snap))).geom geom 
			FROM param,
			(
				SELECT rpg.id, typ, st_diff_approx(rpg.geom, st_union(
	    				CASE WHEN r.geom_buf IS null 
					THEN st_geomfromtext('GEOMETRYCOLLECTION EMPTY') 
					ELSE r.geom_buf 
					END)) 
					AS geom
				FROM param, rpg_left_right rpg 
				LEFT JOIN voie_buf r 
				on st_intersects(rpg.geom, r.geom_buf)
				GROUP BY rpg.id, typ, rpg.geom
			) t1
		) t2
		WHERE st_area(geom) > 0.1;
	ALTER TABLE rpg_left_right_ssvoie ADD CONSTRAINT rpg_left_right_ssvoie_pk PRIMARY KEY(id);
	CREATE INDEX sidx_rpg_left_right_ssvoie_geom ON rpg_left_right_ssvoie USING gist (geom);
	ANALYZE rpg_left_right_ssvoie;
	SELECT msg('END sous-partie 4.4 RPG');	
	COMMIT;


	BEGIN;
	-- supprime le recouvrement entre les champs
	DROP TABLE IF EXISTS inter;
	CREATE TEMPORARY TABLE inter 
	AS
		SELECT CASE 
			WHEN rpg1.typ < rpg2.typ 
			THEN rpg1.id 
			ELSE rpg2.id 
		END idadd, 
			CASE WHEN rpg1.typ < rpg2.typ 
			THEN rpg2.id 
			ELSE rpg1.id 
		END idrem, 
		st_collectionExtract(st_intersection(rpg1.geom, rpg2.geom), 3) geom
		FROM rpg_left_right_ssvoie rpg1, rpg_left_right_ssvoie rpg2
		WHERE rpg1.id < rpg2.id AND st_intersects(rpg1.geom, rpg2.geom);
	ANALYZE inter;
	SELECT msg('END sous-partie 4.5 RPG');
	COMMIT;

	--Application du résultat de l'étape précédante sur le RPG_sans_les_voies
	BEGIN;
	DROP TABLE IF EXISTS rpg_inter;
	CREATE TABLE rpg_inter 
	AS 
		SELECT row_number() over () id, typ, geom 
		FROM (
			SELECT typ, (st_dump(st_clean_poly(geom, snap))).geom geom
			FROM param, 
			(
				SELECT rpgadd.id, rpgadd.typ, st_diff_approx(rpgadd.geom, st_union(
					CASE WHEN inter.geom IS null 
					THEN st_geomfromtext('MULTIPOLYGON EMPTY') 
					ELSE inter.geom 
					END)
					) geom
				FROM rpg_left_right_ssvoie rpgadd 
				LEFT JOIN inter 
				on rpgadd.id = inter.idrem
				GROUP BY rpgadd.id, rpgadd.typ, rpgadd.geom
			) t1
		WHERE not (st_isempty(geom) or geom IS null) 
		) t2;

	ALTER TABLE rpg_inter ADD CONSTRAINT rpg_inter_pk PRIMARY KEY(id);
	CREATE INDEX sidx_rpg_inter_geom ON rpg_inter USING gist (geom);
	ANALYZE rpg_inter;
	SELECT msg('END sous-partie 4.6 RPG');
	COMMIT;
	
	
	BEGIN;
	--/ OIGNONS
	--comblement itératif (jusqu'a 5 oignons)
	SELECT msg('start oignon 4.7 on RPG');
	DROP TABLE IF EXISTS rpg5;
	CREATE TABLE rpg5 
	AS
		SELECT id, typ, st_clean_poly(st_buffer(geom, prec), snap) geom
		FROM rpg_inter, param;
	ALTER TABLE rpg5 ADD CONSTRAINT rpg5_pk PRIMARY KEY(id);
	CREATE INDEX sidx_rpg5_geom ON rpg5 USING gist (geom);
	ANALYZE rpg5;
	COMMIT;

BEGIN;

	SELECT msg('déclaration de la fonction oignon');

	CREATE OR REPLACE FUNCTION test(n_oignon INT) 
	RETURNS void AS 
	$$
	BEGIN

	  IF n_oignon > 0 THEN
		raise notice 'oignons_1';
		DROP TABLE IF EXISTS rpg_buf_oignon;
		CREATE TABLE rpg_buf_oignon
		  AS
			SELECT 0.50 AS r, rpg.id, rpg.typ, st_clean_poly(
			st_buffer(rpg.geom, 0.50), snap
			) geom
			FROM rpg_inter rpg, param;
	  ELSE
		raise notice 'stop 1';
	  END IF;
	
	  IF n_oignon > 1 THEN
		raise notice 'oignons_2';
		DROP TABLE IF EXISTS oignon_2;
		CREATE TABLE oignon_2
		  AS
			SELECT 1.00 AS r, rpg.id, rpg.typ, st_clean_poly(
			st_buffer(rpg.geom, 1.00), snap
			) geom
			FROM rpg_inter rpg, param;
		INSERT INTO rpg_buf_oignon( SELECT * FROM oignon_2);
	  ELSE
		raise notice 'stop 2';
	  END IF;
	  IF n_oignon > 2 THEN
		raise notice 'oignons_3';
		DROP TABLE IF EXISTS oignon_3;
		CREATE TABLE oignon_3
		  AS
			SELECT 1.50 AS r, rpg.id, rpg.typ, st_clean_poly(
			st_buffer(rpg.geom, 1.50), snap
			) geom
			FROM rpg_inter rpg, param;
		INSERT INTO rpg_buf_oignon( SELECT * FROM oignon_3);
	  ELSE
		raise notice 'stop 3';
	  END IF;
	  IF n_oignon > 3 THEN
		raise notice 'oignons_4';
		DROP TABLE IF EXISTS oignon_4;
		CREATE TABLE oignon_4
		  AS
			SELECT 2.00 AS r, rpg.id, rpg.typ, st_clean_poly(
			st_buffer(rpg.geom, 2.00), snap
			) geom
			FROM rpg_inter rpg, param;
		INSERT INTO rpg_buf_oignon( SELECT * FROM oignon_4);
	  ELSE
		raise notice 'stop 4';
	  END IF;
	  IF n_oignon > 4 THEN
		raise notice 'oignons_5';
		DROP TABLE IF EXISTS oignon_5;
		CREATE TABLE oignon_5
		  AS
			SELECT 2.50 AS r, rpg.id, rpg.typ, st_clean_poly(
			st_buffer(rpg.geom, 2.50), snap
			) geom
			FROM rpg_inter rpg, param;
		INSERT INTO rpg_buf_oignon( SELECT * FROM oignon_5);
	  ELSE
		raise notice 'stop 5';
	  END IF;	  
	  IF n_oignon > 5 THEN
		raise notice 'oignons_6';
		DROP TABLE IF EXISTS oignon_6;
		CREATE TABLE oignon_6
		  AS
			SELECT 3.00 AS r, rpg.id, rpg.typ, st_clean_poly(
			st_buffer(rpg.geom, 3.00), snap
			) geom
			FROM rpg_inter rpg, param;
		INSERT INTO rpg_buf_oignon( SELECT * FROM oignon_6);
	  ELSE
		raise notice 'stop 6';
	  END IF;
	END;
	$$ LANGUAGE plpgsql;

	SELECT test(n_oignon) FROM param;
	CREATE INDEX sidx_rpg_buf_oignon_geom ON rpg_buf_oignon USING gist (geom);

COMMIT;

	BEGIN;
	-- intersecte les buffers avec rpg5
	DROP TABLE IF EXISTS rpg_int_oignon;
	CREATE TABLE rpg_int_oignon 
	AS
		SELECT r, rpg.id, rpg.typ, st_collectionExtract(st_union(
		st_intersection(rpg5.geom, rpg.geom)), 3) geom
		FROM rpg_buf_oignon rpg, rpg5, param
		WHERE 
			CASE 
				WHEN param.nature_troncon_route < 10 
				THEN st_intersects(rpg5.geom, rpg.geom)  
				ELSE rpg.typ != rpg5.typ AND st_intersects(rpg5.geom, rpg.geom)
				END
		GROUP BY r, rpg.id, rpg.typ;
	ALTER TABLE rpg_int_oignon ADD CONSTRAINT rpg_int_oignon_pk PRIMARY KEY(r, id);
	CREATE INDEX sidx_rpg_int_oignon_geom ON rpg_int_oignon USING gist (geom);
	ANALYZE rpg_int_oignon;
	SELECT msg('END sous-partie oignon 4.7.1 RPG');	
	COMMIT;


	BEGIN;
	-- enlève le recouvrement entre les champs pour tous les rayons
	DROP TABLE IF EXISTS inter;
	CREATE TEMPORARY TABLE inter 
	AS
		SELECT CASE 
			WHEN rpg1.r < rpg2.r 
			THEN rpg1.r 
			ELSE rpg2.r 
			END radd,  
		CASE 
			WHEN rpg1.r < rpg2.r 
			THEN rpg2.r 
			ELSE rpg1.r 
			END rrem,  
       		CASE 
			WHEN rpg1.r < rpg2.r 
			THEN rpg1.id 
			WHEN rpg1.r > rpg2.r 
			THEN rpg2.id 
			WHEN rpg1.typ < rpg2.typ 
			THEN rpg1.id 
			ELSE rpg2.id 
			END idadd, 
       		CASE 
			WHEN rpg1.r < rpg2.r 
			THEN rpg2.id 
			WHEN rpg1.r > rpg2.r 
			THEN rpg1.id 
			WHEN rpg1.typ < rpg2.typ 
			THEN rpg2.id 
			ELSE rpg1.id 
			END idrem, 
		st_makevalid_poly(st_intersection(rpg1.geom, rpg2.geom)) geom
		FROM rpg_int_oignon rpg1, rpg_int_oignon rpg2
		WHERE rpg1.r <= rpg2.r AND rpg1.id < rpg2.id AND st_intersects(rpg1.geom, rpg2.geom);
	ANALYZE inter;

	--Join les oignons au RPG_inter
	DROP TABLE IF EXISTS rpg_oign;
	CREATE TEMPORARY TABLE rpg_oign AS 
	SELECT *
	FROM (
		SELECT rpg.r, rpg.id, rpg.typ, st_clean_poly(st_diff_approx(rpg.geom, st_union(
			CASE WHEN inter.geom IS null 
			THEN st_geomfromtext('MULTIPOLYGON EMPTY') 
			ELSE inter.geom 
			END
			)), snap) geom
		FROM param, rpg_int_oignon rpg 
		LEFT JOIN inter 
		ON rpg.r = inter.rrem and rpg.id = inter.idrem
		GROUP BY rpg.r, rpg.id, rpg.typ, rpg.geom, snap
	) t1
	WHERE not (st_isempty(geom) or geom IS null);
	ALTER TABLE rpg_oign ADD CONSTRAINT rpg_oign_pk PRIMARY KEY(r, id);
	CREATE INDEX sidx_rpg_oign_geom ON rpg_oign USING gist (geom);
	ANALYZE rpg_oign;
	SELECT msg('END sous-partie oignon 4.7.2 RPG');		
	COMMIT;

	BEGIN;
	-- supprime les superpositions entre les différents rayons d'un même champs
	-- combine les différents oignons
	DROP TABLE IF EXISTS rpg15;
	CREATE TEMPORARY TABLE rpg15 
	AS
		SELECT rpg.id, rpg.typ, st_clean_poly(st_union_approx(rpg1.geom, st_clean_poly(
		st_union_approx(st_collect(rpg.geom)), snap)), snap) geom
		FROM param,
		(
			SELECT rpg_2.r, rpg_2.id, rpg_2.typ, st_diff_approx(rpg_2.geom,
			st_union_approx(st_collect(rpg_1.geom))) geom
			FROM rpg_oign rpg_2 
			LEFT JOIN rpg_oign rpg_1 
			on st_intersects(rpg_2.geom, rpg_1.geom) AND rpg_2.r = rpg_1.r+1
			WHERE rpg_2.r > 1 
			GROUP BY rpg_2.r, rpg_2.id, rpg_2.typ, rpg_2.geom
		) rpg 
		LEFT JOIN 
		rpg_oign rpg1 
		on rpg.id = rpg1.id
		WHERE not(rpg.geom IS null or st_isempty(rpg.geom)) and rpg1.r = 1
		GROUP BY rpg.id, rpg.typ, rpg1.geom, snap;
	ALTER TABLE rpg15 ADD CONSTRAINT rpg15_pk PRIMARY KEY(id);
	CREATE INDEX sidx_rpg15_geom ON rpg15 USING gist (geom);
	ANALYZE rpg15;
	SELECT msg('END sous-partie oignon 4.7.3 RPG');	
	COMMIT;

	BEGIN;
	-- enlève les vrais champs des oignons
	-- combine les oignons au champs correspondant
	DROP TABLE IF EXISTS rpg_comb;
	CREATE TEMPORARY TABLE rpg_comb 
	AS
		SELECT rpg.id, rpg.typ, st_clean_poly(st_union_approx(rpg.geom, 
			CASE WHEN rpg15_.geom IS null 
			THEN st_geomfromtext('GEOMETRYCOLLECTION EMPTY') 
			ELSE rpg15_.geom 
			END), snap) geom
		FROM param, rpg_inter rpg 
		LEFT JOIN
		(
			SELECT rpg15.id, rpg15.typ, st_diff_approx(rpg15.geom, st_makevalid_poly(
			st_union_approx(st_collect(rpg.geom)))) geom
			FROM rpg15, rpg_inter rpg
			WHERE st_intersects(rpg15.geom, rpg.geom)
			GROUP BY rpg15.id, rpg15.typ, rpg15.geom
		) rpg15_ 
		on rpg.id = rpg15_.id;
	ALTER TABLE rpg_comb ADD CONSTRAINT rpg_comb_pk PRIMARY KEY(id);
	CREATE INDEX sidx_rpg_comb_geom ON rpg_comb USING gist (geom);
	ANALYZE rpg_comb;
	SELECT msg('END sous-partie oignon 4.7.4 RPG');	
	COMMIT;

	BEGIN;
	-- buffer +prec sur les vrais champs
	-- Nécessaire pour l'analyse de voisinage dans des étapes ultérieures
	DROP TABLE IF EXISTS rpgbuf;
	CREATE TEMPORARY TABLE rpgbuf 
	AS
		SELECT id, st_makevalid_poly(st_buffer(geom, prec/2)) geom
		FROM rpg_inter, param; 
	CREATE INDEX sidx_rpgbuf_geom ON rpgbuf USING gist (geom);
	ANALYZE rpgbuf;

	--Nettoyage des polygone en 3 étapes (/!\ écorne de 0,1mm les sommets des polygones)
	--Nécessaire car des problèmes récurents lors de la création des polygones
	--Obligation de procéder en 3 étapes car dilatation/érosion insuffisant
	--1 demi-dillatation
	--2 érosion
	--3 demi-dillatation(retour au bordure d'origine)
	DROP TABLE IF EXISTS rpg_clean;
	CREATE TEMPORARY TABLE rpg_clean 
	AS
		SELECT id, typ, st_buffer(st_buffer(st_buffer(rpg_comb.geom, 2*snap),
		 -4*snap), 2*snap) geom
		FROM rpg_comb, param;
	ALTER TABLE rpg_clean ADD CONSTRAINT rpg_clean_pk PRIMARY KEY(id);
	CREATE INDEX sidx_rpg_clean_geom ON rpg_clean USING gist (geom);
	ANALYZE rpg_clean;

	DROP TABLE IF EXISTS rpg_final;
	CREATE TABLE rpg_final 
	AS
		SELECT row_number() over () id, typ, geom, st_buffer(geom, prec) geom_buf -- /!\ MAJ 			prec a remplacé 4.9 --
		FROM (
			SELECT rpg_clean.id, typ, st_clean_poly(st_inter_approx(rpg_clean.geom,
			st_clean_poly(st_buffer(st_union(rpgbuf.geom), -prec/2), snap)), snap) geom
			FROM rpg_clean, rpgbuf, param
			WHERE st_intersects(rpg_clean.geom, rpgbuf.geom)
			GROUP BY rpg_clean.id, typ, rpg_clean.geom, snap, prec
			) t, param  				--/!\--MODIF du 27 01 2010 AJOUT param
		WHERE st_area(geom) > 0.1;
	ALTER TABLE rpg_final ADD CONSTRAINT rpg_final_pk PRIMARY KEY(id);
	CREATE INDEX sidx_rpg_final_geom ON rpg_final USING gist (geom);
	CREATE INDEX sidx_rpg_final_geom_buf ON rpg_final USING gist (geom_buf);
	ANALYZE rpg_final;
	SELECT msg('END sous-partie oignon 4.7.5 RPG');	
	SELECT msg('END oignon 4.7 RPG');	
	COMMIT;


	
	-- Eau_RPG colle le rpg à l'eau
	BEGIN;
	DROP TABLE IF EXISTS rpg_eau;
	CREATE TEMPORARY TABLE rpg_eau 
	AS
		SELECT rpg4.id, rpg4.typ, st_makevalid_poly(st_buffer(st_buffer(st_union(st_makevalid(
		st_union(rpg4.geom, 
			CASE WHEN int_buf.geom IS null 
			THEN st_geomfromtext('MULTIPOLYGON EMPTY') 
			ELSE int_buf.geom 
			END
		))), -prec/2), prec/2)) geom
		FROM param, rpg_final rpg4 

		LEFT JOIN
		(
			SELECT rpg4.id, (st_dump(st_inter_approx(rpg4.geom_buf, 
			st_buffer(eau.geom, prec)))).geom geom
			FROM param, rpg_final rpg4, eau
			WHERE st_intersects(rpg4.geom_buf, eau.geom) 
		) int_buf 
		on int_buf.id=rpg4.id
		GROUP BY rpg4.id, rpg4.typ, prec;
	ALTER TABLE rpg_eau ADD CONSTRAINT rpg_eau_pk PRIMARY KEY(id);
	CREATE INDEX sidx_rpg_eau_geom ON rpg_eau USING gist (geom);
	ANALYZE rpg_eau;
	SELECT msg('END sous-partie 4.8 RPG');	
	COMMIT;

	BEGIN;
	-- supprime le recouvrement entre les champs
	DROP TABLE IF EXISTS inter;
	CREATE TEMPORARY TABLE inter 
	AS
		SELECT CASE 
			WHEN rpg1.typ < rpg2.typ 
			THEN rpg1.id 
			ELSE rpg2.id 
			END idadd, 
		CASE 
			WHEN rpg1.typ < rpg2.typ 
			THEN rpg2.id 
			ELSE rpg1.id 
			END idrem, 
		st_makevalid_poly(st_intersection(rpg1.geom, rpg2.geom)) geom
		FROM rpg_eau rpg1, rpg_eau rpg2
		WHERE rpg1.id < rpg2.id and st_intersects(rpg1.geom, rpg2.geom);
	ANALYZE inter;

	DROP TABLE IF EXISTS rpg_eau_inter;
	CREATE TEMPORARY TABLE rpg_eau_inter 
	AS 
		SELECT rpgadd.id, rpgadd.typ, st_clean_poly(st_diff_approx(rpgadd.geom, st_union(
			CASE WHEN inter.geom IS null 
			THEN st_geomfromtext('MULTIPOLYGON EMPTY') 
			ELSE inter.geom 
			END
		)), snap) geom
		FROM param, rpg_eau rpgadd 
		
		LEFT JOIN 
		inter 	
		on rpgadd.id = inter.idrem
		GROUP BY rpgadd.id, rpgadd.typ, rpgadd.geom, snap;

	ALTER TABLE rpg_eau_inter ADD CONSTRAINT rpg_eau_inter_pk PRIMARY KEY(id);
	CREATE INDEX sidx_rpg_eau_inter_geom ON rpg_eau_inter USING gist (geom);
	SELECT msg('END sous-partie 4.9 RPG');	
	ANALYZE rpg_eau_inter;

	-- union rpg_eau_inter avec rpg_final
	DROP TABLE IF EXISTS rpg_final_eau;
	CREATE TEMPORARY TABLE rpg_final_eau 
	AS 
		SELECT row_number() over () id, typ, st_clean_poly(geom, snap) geom
		FROM param,
		(
			SELECT rpg.id, rpg.typ, st_diff_approx(rpg.geom, st_union_approx(st_collect(
				CASE WHEN eau.geom IS null 
				THEN st_geomfromtext('MULTIPOLYGON EMPTY') 
				ELSE eau.geom 
				END))) geom
				FROM param, 
				(
					SELECT rpg_final.id, rpg_final.typ, (st_dump(st_clean_poly(
					st_union_approx(rpg_final.geom,rpg_eau.geom),snap))).geom geom
					FROM param, rpg_eau_inter rpg_eau, rpg_final
					WHERE rpg_eau.id = rpg_final.id
				) rpg 

				LEFT JOIN 
				eau 
				on st_intersects(rpg.geom, eau.geom)
				GROUP BY rpg.id, rpg.typ, rpg.geom
		) t
		WHERE st_area(geom) > 0.1;

	ALTER TABLE rpg_final_eau ADD CONSTRAINT rpg_final_eau_pk PRIMARY KEY(id);
	CREATE INDEX sidx_rpg_final_eau_geom ON rpg_final_eau USING gist (geom);
	ANALYZE rpg_final_eau;
COMMIT;

BEGIN;
	DROP TABLE IF EXISTS rpg_final_t;
	CREATE TABLE rpg_final_t 
	AS 
		SELECT row_number() over () id, typ, geom
		FROM (
			SELECT rpg.typ, st_makevalid_poly((st_dump(st_intersection(rpg.geom,
			 grid.geom))).geom) geom
			FROM rpg_final_eau rpg, grid
			WHERE st_intersects(rpg.geom, grid.geom)
		) t;

	ALTER TABLE rpg_final_t ADD CONSTRAINT rpg_final_t_pk PRIMARY KEY(id);
	CREATE INDEX sidx_rpg_final_t_geom ON rpg_final_t USING gist (geom);
	ANALYZE rpg_final_t;

	SELECT msg('END sous-partie 4.10 RPG');
	SELECT msg('END partie 4 RPG');	
	COMMIT;

			BEGIN;
			SELECT msg('rpg_t');
	DROP TABLE IF EXISTS rpg_t;
	CREATE TABLE rpg_t
	AS 
		SELECT id, typ, st_force2d(geom) geom
		FROM rpg_final_t; 
	CREATE INDEX sidx_rpg_t_geom ON rpg_t USING gist (geom);
	ALTER TABLE rpg_t ADD CONSTRAINT rpg_t_pk PRIMARY KEY (id);
	ANALYZE rpg_t;
	COMMIT;

		BEGIN;
			SELECT msg('r_t');
	DROP TABLE IF EXISTS r_t;
	CREATE TABLE r_t
	AS 
		SELECT st_force2d(geom) geom
		FROM select_tot_vf_r;
	CREATE INDEX sidx_r_t_geom ON r_t USING gist (geom);
	ANALYZE r_t;
	COMMIT;
	
	BEGIN;
			SELECT msg('rpg_final_t2');
	--/Suppression des supperpositions entre route et champs 
	--/Permet de préparer les champs à l'étape suivante
	DROP TABLE IF EXISTS rpg_final_t2;
	CREATE TABLE rpg_final_t2
	AS 

				SELECT rpg.id, typ, st_difference(rpg.geom, st_union(
						CASE WHEN r.geom IS null 
						THEN st_geomfromtext('GEOMETRYCOLLECTION EMPTY') 
						ELSE r.geom END))
				AS geom
				FROM param, rpg_t rpg 
				LEFT JOIN r_t r 
				ON st_intersects(r.geom, rpg.geom)
				GROUP BY rpg.id, typ, rpg.geom;

	COMMIT;
	BEGIN;
			SELECT msg('rpg_final');
	--/Suppression des supperpositions entre route et champs 
	--/Permet de préparer les champs à l'étape suivante
	DROP TABLE IF EXISTS rpg_final;
	CREATE TABLE rpg_final
	AS 
		SELECT row_number() over () id, *
		FROM ( 
			-- single part
			SELECT typ, st_clean_poly((st_dump(geom)).geom,snap) geom 
			FROM rpg_final_t2 t1, param
		) t2
		WHERE st_area(geom)>500;

	ALTER TABLE rpg_final ADD CONSTRAINT rpg_final_pk PRIMARY KEY(id);
	CREATE INDEX sidx_rpg_final_geom ON rpg_final USING gist (geom);
	ANALYZE rpg_final;
	SELECT msg('END sous-partie 4.2 RPG');
	COMMIT;







--
--/SCRIPT PARTIE 5 FORET
--


	-- Grille deux en decallee

	BEGIN;
	SELECT msg('Start FORET');
	DROP TABLE IF EXISTS zone5 CASCADE;
	CREATE TABLE zone5 
	AS
		SELECT st_buffer(zon.geom, (gridsize/2)) geom
		FROM zone_buf zon, param;

	SELECT msg('Start grille forestière 1/2');
	DROP TABLE IF EXISTS grid4;
	CREATE TABLE grid4 
	AS
		SELECT row_number() over () id, g.geom
		FROM (
			SELECT ST_Translate(st_scale(cell, gridsize, gridsize), i * gridsize + x1, 
			j * gridsize + y1) AS geom
			FROM param,
			(
				SELECT ('SRID=2154;POLYGON((0 0, 0 1, 1 1, 1 0,0 0))')::geometry 
				AS cell
			) t,
			(
				SELECT st_xmin(geom)::integer x1, st_ymin(geom)::integer y1 
				FROM zone5
			) extent,
			generate_series(0, 
				(SELECT ceil(st_xmax(geom))::integer-st_xmin(geom)::integer 
				FROM zone5)/(SELECT gridsize FROM param)) 
				AS i, 
			generate_series(0, 
				(SELECT ceil(st_ymax(geom))::integer-st_ymin(geom)::integer 
				FROM zone5)/(SELECT gridsize FROM param)) 
				AS j 
		) g, zone5 z;

	ALTER TABLE grid4 ADD CONSTRAINT grid4_pk PRIMARY KEY(id);
	CREATE INDEX sidx_grid4_geom ON grid4 USING gist (geom);
	ANALYZE grid4; 

	SELECT msg('Start grille forestière 2/2');
	DELETE FROM grid4
	WHERE grid4.id 
	IN (
		SELECT grid4.id 
		FROM grid4, zone5 
		WHERE NOT st_intersects(grid4.geom, zone5.geom)
	);


	UPDATE grid4 
	SET geom = st_makevalid_poly(st_intersection(grid4.geom, zone5.geom))
	FROM zone5
	WHERE st_overlaps(grid4.geom, zone5.geom);

	ANALYZE grid4; 
	COMMIT;

 	-- Grille deux en decallee

	BEGIN;
	SELECT msg('Start vege ss err FORET 1/2');
	DROP TABLE IF EXISTS eau_route;
	CREATE TABLE eau_route
	AS 
    		SELECT geom
		FROM select_tot_vf_r
	;
	INSERT INTO eau_route  (
		SELECT geom 
		FROM eau
	);
		INSERT INTO eau_route  (   --modif du 27/05/2015
		SELECT geom 
		FROM rpg_final
	);
	
	CREATE INDEX sidx_eau_route_geom ON eau_route USING gist (geom);
	ANALYSE eau_route;

	-- extraction de la foret sur la zone
	-- suppression des routes eau champs de la forêt
	-- buffer de prec/2 sans union
	SELECT msg('Start vege ss err FORET 2/2');
	DROP TABLE IF EXISTS vege_ss_err;
	CREATE TABLE vege_ss_err 
	AS 
		SELECT row_number() over () id, st_buffer(geom, prec/2) geom
		FROM param, (
			SELECT (st_dump(t1.geom)).geom geom 
			FROM (
				SELECT vege.id, st_diff_approx(vege.geom, st_union(
	    				CASE WHEN err.geom IS null 
					THEN st_geomfromtext('GEOMETRYCOLLECTION EMPTY') 
					ELSE err.geom 
					END)) AS geom
				FROM zone5,zone_vegetation vege 
				LEFT JOIN 
				eau_route err 
				ON st_intersects(vege.geom, err.geom)
				WHERE st_intersects(zone5.geom, vege.geom)
				GROUP BY vege.id, vege.geom
			) t1
		) t2;
	ALTER TABLE vege_ss_err ADD CONSTRAINT vege_ss_err_pk PRIMARY KEY(id);
	CREATE INDEX sidx_vege_ss_err_geom ON vege_ss_err USING gist (geom);
	ANALYZE vege_ss_err;
	COMMIT;
	

 	-- DEBUT DU comblement des clairieres

	-- recoupe la foret sur 2 grilles 
	-- MERGE DES DEUX DF et buffer négatif
	BEGIN;
	SELECT msg('Start clairière FORET 1/3');
	DROP TABLE IF EXISTS df12;
	CREATE TABLE df12 
	AS
    		SELECT st_makevalid_poly(st_buffer(geom, -(prec/2))) geom 
    		FROM (
			SELECT st_makevalid_poly((st_dump(geom)).geom)  geom
			FROM (
        			SELECT grid.id, st_union(st_intersection(vsserr.geom, grid.geom)) geom
				FROM vege_ss_err vsserr, grid, param
				WHERE st_intersects(vsserr.geom, grid.geom)
        			GROUP BY grid.id
   			) t
    		) t1, param;
	SELECT msg('Start clairière FORET 2/3');
	INSERT INTO df12 
    		SELECT st_makevalid_poly(st_buffer(geom, -(prec/2))) geom 
    		FROM (
			SELECT st_makevalid_poly((st_dump(geom)).geom)  geom
			FROM (
        			SELECT grid4.id, st_union(
				st_intersection(vsserr.geom, grid4.geom)
				) geom
				FROM vege_ss_err vsserr, grid4, param
				WHERE st_intersects(vsserr.geom, grid4.geom)
        			GROUP BY grid4.id
    			) t
    		) t1, param;
	CREATE INDEX sidx_df12_geom ON df12 USING gist (geom);
	COMMIT;

	--Union sur la grille
	BEGIN;
	SELECT msg('Start clairière FORET 3/3');
	DROP TABLE IF EXISTS vege2;
	CREATE TABLE vege2 
	AS 
		SELECT row_number() over() id, geom, 
		st_makevalid_poly(st_buffer(geom, prec/2)) geom_buf
		FROM param, ( 
    			SELECT (st_dump(geom)).geom geom 
    			FROM (
        			SELECT grid.id, st_makevalid_poly(st_union_approx(st_collect(
				st_intersection(df12.geom, grid.geom)
				))) geom
        			FROM df12, grid, param
        			WHERE st_intersects(df12.geom, grid.geom)
        			GROUP BY grid.id
    			) t1
		) t2;    

	CREATE INDEX vege2_geom ON vege2 USING gist(geom);
	CREATE INDEX vege2_buf_geom ON vege2 USING gist(geom_buf);
	ALTER TABLE vege2 ADD CONSTRAINT vege2_pk PRIMARY KEY(id);

	COMMIT;


	-- foret collé


	BEGIN;
	SELECT msg('Start FORET colle 1/3');
	DROP TABLE IF EXISTS foret_colle;
	CREATE TABLE foret_colle 
	AS 
		SELECT vserr.id, st_union(st_makevalid_poly(st_union(vserr.geom, 
			CASE WHEN int_buf.geom IS null 
			THEN st_geomfromtext('GEOMETRYCOLLECTION EMPTY') 
			ELSE int_buf.geom 
			END))) geom
		FROM param, vege2 vserr 
		LEFT JOIN 
		(
			SELECT id, st_makevalid_poly((st_dump(
			st_inter_approx(vserr.geom_buf, st_buffer(err.geom, prec/2))
			)).geom) geom
			FROM vege2 vserr, eau_route err, param
			WHERE st_intersects(vserr.geom_buf, err.geom)
		) int_buf 
		on int_buf.id=vserr.id
		GROUP BY vserr.id, prec
		;

	ALTER TABLE foret_colle ADD CONSTRAINT foret_colle_pk PRIMARY KEY(id);
	CREATE INDEX sidx_foret_colle_geom ON foret_colle USING gist (geom);
	ANALYZE foret_colle;
	COMMIT;
	
	BEGIN;
	SELECT msg('Start FORET colle 2/3');
	-- supprime le recouvrement entre les champs
	DROP TABLE IF EXISTS inter;
	CREATE TEMPORARY TABLE inter 
	AS
		SELECT CASE 
			WHEN f1.id < f2.id 
			THEN f1.id 
			ELSE f2.id 
			END idadd, 
		CASE 
			WHEN f1.id < f2.id 
			THEN f2.id 
			ELSE f1.id 
			END idrem,
		st_makevalid_poly(st_intersection(f1.geom, f2.geom)) geom
		FROM foret_colle f1, foret_colle f2
		WHERE f1.id < f2.id and st_intersects(f1.geom, f2.geom);
	ANALYZE inter;

	SELECT msg('Start FORET colle 3/3');
	DROP TABLE IF EXISTS f_inter;
	CREATE TEMPORARY TABLE f_inter 
	AS 
		SELECT fadd.id, st_clean_poly(st_diff_approx(fadd.geom, st_union(
			CASE WHEN inter.geom IS null 
			THEN st_geomfromtext('MULTIPOLYGON EMPTY') 
			ELSE inter.geom 
			END
		)), snap) geom
		FROM param, foret_colle fadd 
		
		LEFT JOIN 
		inter 	
		on fadd.id = inter.idrem
		WHERE st_area(fadd.geom) > 50
		GROUP BY fadd.id, fadd.geom, snap;

	ALTER TABLE f_inter ADD CONSTRAINT f_inter_pk PRIMARY KEY(id);
	CREATE INDEX sidx_f_inter_geom ON f_inter USING gist (geom);
	ANALYZE f_inter;
	COMMIT;
	
	BEGIN;
	-- enlève err à foret collé et remet en single part
	SELECT msg('Start FORET Final');
	DROP TABLE IF EXISTS foret_final;
	CREATE TABLE foret_final 
	AS
		SELECT row_number() over () id, st_clean_poly(geom, snap) geom
		FROM param, 
		(
			SELECT  foret.id, (st_dump(st_diff_approx(foret.geom, st_union(
				CASE WHEN err1.geom IS null 
				THEN st_geomfromtext('MULTIPOLYGON EMPTY') 
				ELSE err1.geom END)))).geom geom
			FROM f_inter foret 
			LEFT JOIN 
			eau_route err1 
			on st_intersects(foret.geom, err1.geom)
			GROUP BY foret.id, foret.geom
		) t
		WHERE st_area(geom) > 50;

	ALTER TABLE foret_final ADD CONSTRAINT foret_final_pk PRIMARY KEY(id);
	CREATE INDEX sidx_foret_final_geom ON foret_final USING gist (geom);
	ANALYZE foret_final;
	COMMIT;


	--Surcouches optionnelles
	BEGIN;
	-- Délimite les espaces hors zone_végétation
	SELECT msg('Start clairière 1/9');
	DROP TABLE IF EXISTS temp1;
		CREATE TEMPORARY TABLE temp1 AS 
	SELECT 
		row_number() over() id, *
		FROM(
			SELECT (st_dump(geom)).geom geom
			FROM( 
				SELECT g.id, st_difference(st_force2D(g.geom),
				st_union(
					CASE WHEN st_force2D(zv.geom) IS NULL 
					THEN st_geomfromtext('GEOMETRYCOLLECTION EMPTY') 
					ELSE st_force2D(zv.geom)END)
			) AS geom
			FROM zone_buf, grid g
			LEFT JOIN zone_vegetation zv
			ON st_intersects(st_force2D(zv.geom),st_force2D(g.geom))
			WHERE st_intersects(zv.geom, zone_buf.geom)
			GROUP BY g.id, g.geom
			
			)t1
		)t2;
	CREATE INDEX sidx_temp1_geom ON temp1 USING gist (geom);

	-- Identifie les espaces hors zone_végétation pouvant faire l'objet de fusion entre voisin 
	SELECT msg('Start clairière 2/9');
	DROP TABLE IF EXISTS temp2;
	CREATE TEMPORARY TABLE temp2 AS 
		SELECT 
		t1.id AS id1, max(t2.id) AS id2, t1.geom
		FROM temp1 t1, temp1 t2
		WHERE t1.id <> t2.id AND
		st_touches(t1.geom, t2.geom)
		GROUP BY t1.id, t1.geom
	;
	
	-- Récupères les petites clairières ne nécessitant pas de fusion
	SELECT msg('Start clairière 3/9');
	DROP TABLE IF EXISTS surc_clairiere;
	CREATE TABLE surc_clairiere AS 
		SELECT
		t1.id, t1.geom, temp2.id2
		FROM param, temp1 t1 
		LEFT JOIN temp2
		on t1.id=temp2.id1
		WHERE temp2.id2 IS NULL
		AND st_area(t1.geom)< t_max_clairiere
	;
	COMMIT;
	
	BEGIN;
	-- Fusionne les morceaux de clairière entre voisin dans un périmètre de 3 à 5 grid 
	--   NB fusionne au minimum le polygone, son voisin, et le voisin de son voisin
	--      -dans le cas d'un polygone fusionné à partir de l'extreme bordure- 
	--      (Début de la fuision à partir d'un polygone choisit aléatoirement)
	--   NB fusionne au maximum le polygone, son voisin de gauche et le voisin 
	--      de son voisin de gauche
	--      ainsi que son voisin de droite, et le voisin de son voisin de droite 
	--      soit une longueur totale d'un maximum de 5 grid
	--      -dans le cas d'un polygone fusionné a partir du centre de la clairière-
	SELECT msg('Start clairière 4/9');
	DROP TABLE IF EXISTS temp3;
	CREATE TEMPORARY TABLE temp3 AS 
	
		SELECT s2.id3 AS id, st_union(s2.geom) AS geom
		FROM(
			SELECT temp1.id AS id1, max(max(s1.id2, s1.id1)) AS id3, temp1.geom
			FROM temp2 s1, temp1
			WHERE temp1.id <> s1.id1 AND
			st_touches(temp1.geom, s1.geom)
			GROUP BY temp1.id, temp1.geom
		)s2
		GROUP BY s2.id3
	;
	CREATE INDEX sidx_temp3_geom ON temp3 USING gist (geom);
	
	-- Identifie les morceaux de clairière pouvant faire l'objet d'une deuxième fusion entre voisin 
	SELECT msg('Start clairière 5/9');
	DROP TABLE IF EXISTS temp4;
	CREATE TEMPORARY TABLE temp4 AS 
		SELECT 
		t4.id AS id1, max(t5.id) AS id2, t4.geom
		FROM temp3 t4, temp3 t5
		WHERE t4.id <> t5.id AND
		st_touches(t4.geom, t5.geom)
		GROUP BY t4.id, t4.geom
	;
	
	-- Récupères les clairières moyenne ne nécessitant qu'une fusion
	SELECT msg('Start clairière 6/9');
	INSERT INTO surc_clairiere (
		SELECT
		t1.id AS id, t1.geom, t2.id1 AS id2
		FROM param, temp3 t1 
		LEFT JOIN temp4 t2
		on t1.id=t2.id1
		WHERE t2.id1 IS NULL
		AND st_area(t1.geom)< t_max_clairiere
	);
	COMMIT;
	
	BEGIN;
	-- Fusionne les morceaux de clairière entre voisin, étend le périmètre jusqu'à 9 à 15 grid
	-- NB même principe que pour temp3 mais appliqué au objet déjà fusionné
	SELECT msg('Start clairière 7/9');
	DROP TABLE IF EXISTS temp5;
	CREATE TEMPORARY TABLE temp5 AS 
	
		SELECT s2.id3 AS id, st_union(s2.geom) AS geom
		FROM(
			SELECT temp4.id1 AS id1, max(max(s1.id2, s1.id1)) AS id3, temp4.geom
			FROM temp4 s1, temp4
			WHERE temp4.id1 <> s1.id1 AND
			st_touches(temp4.geom, s1.geom)
			GROUP BY temp4.id1, temp4.geom
		)s2
		GROUP BY s2.id3
	;
	
	CREATE INDEX sidx_temp5_geom ON temp5 USING gist (geom);
	
	-- Identifie les morceaux nécessitant trop de fusion pour devoir être des clairières
	SELECT msg('Start clairière 8/9');
	DROP TABLE IF EXISTS temp6;
	CREATE TEMPORARY TABLE temp6 AS 
		SELECT 
		t4.id AS id1, max(t5.id) AS id2, t4.geom
		FROM temp5 t4, temp5 t5
		WHERE t4.id <> t5.id AND
		st_touches(t4.geom, t5.geom)
		GROUP BY t4.id, t4.geom
	;
	
	
	-- Récupères les clairières complexe nécessitant deux fusions
	SELECT msg('Start clairière 9/9');
	INSERT INTO surc_clairiere (
		SELECT
		t1.id AS id, t1.geom, t2.id1 AS id2
		FROM param, temp5 t1 
		LEFT JOIN temp6 t2
		on t1.id=t2.id1
		WHERE t2.id1 IS NULL
		AND st_area(t1.geom)< t_max_clairiere
	);
	
	COMMIT;
	

--
--/SCRIPT PARTIE 6 VILLE
--


	--VILLE (MOdele sous_test7_e52)
	-- Creation de la tache urbaine

	BEGIN;
	SELECT msg('Start urban');
	DROP TABLE IF EXISTS base_urbain;
	CREATE TABLE base_urbain 
	AS 
		SELECT ur.geom 
		FROM bati_indifferencie ur, zone_buf z
		WHERE st_intersects(ur.geom, z.geom);
	INSERT INTO base_urbain  (
        	SELECT ur.geom 
		FROM bati_industriel ur, zone_buf z
		WHERE st_intersects(ur.geom, z.geom));
	INSERT INTO base_urbain (		 
		SELECT ur.geom 
		FROM bati_remarquable ur, zone_buf z
		WHERE st_intersects(ur.geom, z.geom));
	INSERT INTO base_urbain (
		SELECT ur.geom 
		FROM cimetiere ur, zone_buf z
		WHERE st_intersects(ur.geom,z.geom));
	INSERT INTO base_urbain (
		SELECT ur.geom 
		FROM piste_aerodrome ur, zone_buf z
		WHERE st_intersects(ur.geom,z.geom));	
	INSERT INTO base_urbain (
		SELECT ur.geom 
		FROM construction_surfacique ur, zone_buf z
		WHERE st_intersects(ur.geom,z.geom));	
	INSERT INTO base_urbain (
		SELECT ur.geom 
		FROM terrain_sport ur, zone_buf z
		WHERE st_intersects(ur.geom,z.geom));	
	CREATE INDEX sidx_base_urbain_geom_buf ON base_urbain USING gist (geom);
	ANALYZE base_urbain;
	COMMIT;

	BEGIN;
	SELECT msg('Start tache urbaine 1/2');
	-- création de la tache urbaine 
	-- redécoupée sur la grille
	DROP TABLE IF EXISTS urbain;
	CREATE TABLE urbain 
	AS
		SELECT row_number() over() id, geom
		FROM (
			SELECT st_makevalid_poly(st_intersection(t1.geom, grid.geom)) geom
			FROM (
				SELECT (st_dump(st_buffer(st_union(
				st_buffer(ur.geom,50::double precision)
				),-40::double precision))).geom AS geom
				FROM base_urbain ur
			) t1, grid
			WHERE st_area(t1.geom) > 1000 AND st_intersects(t1.geom, grid.geom)
		) t2;
	ALTER TABLE urbain ADD CONSTRAINT urbain_pk PRIMARY KEY(id);
	CREATE INDEX sidx_urbain_geom ON urbain USING gist (geom);
	ANALYZE urbain;

	-- tache urbaine sans les voies
	-- suppression des surfaces recouvrant des voies
	SELECT msg('Start tache urbaine 2/2');
	DROP TABLE IF EXISTS urbain_ssvoie;
	CREATE TABLE urbain_ssvoie 
	AS 
		SELECT row_number() over () id, geom, st_buffer(geom, prec) geom_buf
		FROM param, 
		( 
			SELECT st_makevalid_poly((st_dump(geom)).geom) geom 
			FROM (
				SELECT urbain.id, st_diff_approx(urbain.geom, st_union(
	    				CASE WHEN r.geom_buf IS null 
					THEN st_geomfromtext('GEOMETRYCOLLECTION EMPTY') 
					ELSE r.geom_buf END)) AS geom
				FROM urbain 
				LEFT JOIN voie_buf r 
				on st_intersects(urbain.geom, r.geom_buf)
				GROUP BY urbain.id, urbain.geom
			) t1
		) t2;

	ALTER TABLE urbain_ssvoie ADD CONSTRAINT urbain_ssvoie_pk PRIMARY KEY(id);
	CREATE INDEX sidx_urbain_ssvoie_geom ON urbain_ssvoie USING gist (geom);
	CREATE INDEX sidx_urbain_ssvoie_geom_buf ON urbain_ssvoie USING gist (geom_buf);
	ANALYZE urbain_ssvoie;

	COMMIT;

	-- Merge des surfaces en eau, des champs avec la foret (alternative possible par postgis non possible dans arcgis) /!\ Ne pas ajouter les routes car utilisation droite/gauche

	
	BEGIN;
	SELECT msg('Start assemblage des couches limitantes');
	DROP TABLE IF EXISTS eau_rpg_foret;
	CREATE TABLE eau_rpg_foret 
	AS 
    		SELECT geom 
		FROM foret_final;
	INSERT INTO eau_rpg_foret (
		SELECT geom 
		FROM eau);
	INSERT INTO eau_rpg_foret (	
		SELECT geom 
		FROM rpg_final);
			
	CREATE INDEX sidx_eau_rpg_foret_geom_buf ON eau_rpg_foret USING gist (geom);
	ANALYZE eau_rpg_foret;


	SELECT msg('Start assemblage des couches limitantes');
	DROP TABLE IF EXISTS route_vf_sr;
	CREATE TABLE route_vf_sr
	AS 
    		SELECT st_force2d(geom_buf_s) AS geom 
		FROM route_select;
	INSERT INTO route_vf_sr (
		SELECT st_force2d(geom_buf_s) AS geom
		FROM vf_select);
	INSERT INTO route_vf_sr (	
		SELECT st_force2d(geom) AS geom 
		FROM surf_route_select);
			
	CREATE INDEX sidx_route_vf_sr_geom ON route_vf_sr USING gist (geom);
	ANALYZE route_vf_sr;


	-----(Utilisation du modele Modèle6_D3)
	-- ville collée aux eau,foret et champs quand distance < prec
	SELECT msg('Start liaison 1/2');
	DROP TABLE IF EXISTS ville5m;
	CREATE TABLE ville5m 
	AS
		SELECT  urb5m.id, st_diff_approx(urb5m.geom, st_union_approx(st_collect(
			CASE WHEN erf1.geom IS null 
			THEN st_geomfromtext('MULTIPOLYGON EMPTY') 
			ELSE erf1.geom END))) geom
		FROM (
			SELECT u.id, st_buffer(st_buffer(st_union(st_makevalid_poly(st_union(u.geom, 					CASE WHEN int_buf.geom IS null 
				THEN st_geomfromtext('MULTIPOLYGON EMPTY') 
				ELSE int_buf.geom END
			))), -prec/2), prec/2) geom
			FROM param, urbain_ssvoie u 
			LEFT JOIN
			(
				SELECT id, (st_dump(st_inter_approx(urb.geom_buf, 
				st_buffer(erf.geom, prec)
				))).geom geom
				FROM urbain_ssvoie urb, eau_rpg_foret erf, param
				WHERE st_intersects(urb.geom_buf, erf.geom)
			) int_buf 
			on int_buf.id=u.id
			GROUP BY u.id, prec
		) urb5m 
		LEFT JOIN 
		eau_rpg_foret erf1 
		on st_intersects(urb5m.geom, erf1.geom)
		GROUP BY urb5m.id, urb5m.geom;

	ALTER TABLE ville5m ADD CONSTRAINT ville5m_pk PRIMARY KEY(id);
	CREATE INDEX sidx_ville5m_geom ON ville5m USING gist (geom);
	ANALYZE ville5m;
	COMMIT;

	BEGIN;
	-- Tache urbaine rattachée aux routes lorsque distance < prec selon gauche droite  
	-- (départ tache urbaine sans les voies)	
	SELECT msg('Start liaison 2/2');
	DROP TABLE IF EXISTS ville_route;
	CREATE TABLE ville_route 
	AS
		SELECT  urbain.id, st_makevalid_poly(st_union(urbain.geom, 
			CASE WHEN urbain_lr.geom IS null 
			THEN st_geomfromtext('GEOMETRYCOLLECTION EMPTY') 
			ELSE urbain_lr.geom END
			)) geom
		FROM urbain_ssvoie urbain 
		LEFT JOIN
		(
			SELECT CASE 
				WHEN urbain_left.id IS null 
				THEN urbain_right.id 
				ELSE urbain_left.id END id, 
    			st_union(
				CASE WHEN urbain_left.geom IS null 
				THEN st_geomfromtext('GEOMETRYCOLLECTION EMPTY') 
				ELSE urbain_left.geom END, 
        			CASE WHEN urbain_right.geom IS null 
				THEN st_geomfromtext('GEOMETRYCOLLECTION EMPTY') 
				ELSE urbain_right.geom END) geom
			FROM (
				SELECT urbain.id, st_makevalid_poly(st_buffer(st_buffer(
				st_union(st_clean_poly(st_union(urbain.geom, int_buf.geom),snap))
				, -prec/2), prec/2)) geom
				FROM urbain_ssvoie urbain, param,
				(
					SELECT rpg_r.id, st_makevalid_poly(st_diff_approx(st_makevalid_poly(
					(st_dump(st_inter_approx(st_buffer(rpg_r.geom, prec),
					r.geom_left))).geom), r.geom_buf)) geom
					FROM urbain_ssvoie rpg_r, voie_buf r, param
					WHERE st_intersects(rpg_r.geom, r.geom_left)
				) int_buf
				WHERE int_buf.id=urbain.id
				GROUP BY urbain.id, prec
			) urbain_left 
			full join
			(
				SELECT urbain.id, st_makevalid_poly(st_buffer(st_buffer(
				st_union(st_clean_poly(st_union(urbain.geom, int_buf.geom),snap))
				, -prec/2), prec/2)) geom
				FROM urbain_ssvoie urbain, param,
				(
					SELECT rpg_r.id, st_makevalid_poly(st_diff_approx(st_makevalid_poly(
					(st_dump(st_inter_approx(st_buffer(rpg_r.geom, prec),
					r.geom_right))).geom), r.geom_buf)) geom
					FROM urbain_ssvoie rpg_r, voie_buf r, param
					WHERE st_intersects(rpg_r.geom, r.geom_right)
				) int_buf
				WHERE int_buf.id=urbain.id
				GROUP BY urbain.id, prec
			) urbain_right 
			on (urbain_left.id = urbain_right.id)
		) urbain_lr 
		on urbain.id = urbain_lr.id;

	ALTER TABLE ville_route ADD CONSTRAINT ville_route_pk PRIMARY KEY(id);
	CREATE INDEX sidx_ville_route_geom ON ville_route USING gist (geom);
	ANALYZE ville_route;
	COMMIT;

	SELECT msg('finalisation URBAIN 1/4');
	-- fusionne ville_route et ville5m et supprime les voies
	DROP TABLE IF EXISTS urbain_ssvoie_t;
	CREATE TABLE urbain_ssvoie_t
	AS
				SELECT st_makevalid_poly(st_union(st_clean_poly(st_buffer(st_buffer(
				st_buffer(urb_route.geom, 2*snap), -4*snap), 2*snap), snap), 
	        		st_clean_poly(st_buffer(st_buffer(
				st_buffer(urb_5m.geom, 2*snap), -4*snap), 2*snap), snap))) geom
				FROM ville_route urb_route, ville5m urb_5m, param
				WHERE urb_route.id = urb_5m.id;
	CREATE INDEX sidx_urbain_ssvoie_t_geom ON urbain_ssvoie_t USING gist (geom);
	SELECT msg('finalisation URBAIN 2/4');
	DROP TABLE IF EXISTS urbain_ssvoie_t2;
	CREATE TABLE urbain_ssvoie_t2 
	AS
			SELECT grid.id, st_makevalid_poly(st_union_approx(st_collect(
			st_makevalid_poly(st_intersection(t1.geom, grid.geom))))) geom
			FROM urbain_ssvoie_t t1, grid 
			WHERE st_intersects(t1.geom, grid.geom) and t1.geom && grid.geom
			GROUP BY grid.id;
	CREATE INDEX sidx_urbain_ssvoie_t2_geom ON urbain_ssvoie_t2 USING gist (geom);
	SELECT msg('finalisation URBAIN 3/4');
	DROP TABLE IF EXISTS urbain_ssvoie;
	CREATE TABLE urbain_ssvoie 
	AS
	SELECT row_number() over () id, geom
	FROM ( 
		SELECT st_makevalid_poly((st_dump(st_diff_approx(t2.geom, st_union(
    			CASE WHEN r.geom IS null 
			THEN st_geomfromtext('GEOMETRYCOLLECTION EMPTY') 
			ELSE r.geom END)))).geom) geom
		FROM urbain_ssvoie_t2 t2 
		LEFT JOIN 
		route_vf_sr r 
		on st_intersects(t2.geom, r.geom)
		GROUP BY t2.geom
	) t3;

	ALTER TABLE urbain_ssvoie ADD CONSTRAINT urbain_ssvoie_pk PRIMARY KEY(id);
	CREATE INDEX sidx_urbain_ssvoie_geom ON urbain_ssvoie USING gist (geom);
	ANALYZE urbain_ssvoie;

	-- tache urbaine sans erf
	SELECT msg('finalisation URBAIN 4/4');
	DROP TABLE IF EXISTS urbain_final;
	CREATE TABLE urbain_final 
	AS 
		SELECT row_number() over () id, st_clean_poly(geom, snap) geom
		FROM param, 
		( 
			SELECT (st_dump(geom)).geom geom 
			FROM (
				SELECT id, st_diff_approx(urbain.geom, st_union_approx(st_collect(
	    				CASE WHEN erf.geom IS null 
					THEN st_geomfromtext('GEOMETRYCOLLECTION EMPTY') 
					ELSE erf.geom END))) AS geom
				FROM urbain_ssvoie urbain 
				LEFT JOIN 
				eau_rpg_foret erf 
				on st_intersects(urbain.geom, erf.geom)
				GROUP BY id, urbain.geom
			) t1
		) t2
		WHERE st_area(geom) > 0.1;

	ALTER TABLE urbain_final ADD CONSTRAINT urbain_final_pk PRIMARY KEY(id);
	CREATE INDEX sidx_urbain_final_geom ON urbain_final USING gist (geom);
	ANALYZE urbain_final;
	SELECT msg('END urban');
	COMMIT;

DROP TABLE IF EXISTS urbain, ville5m, ville_route, urbain_ssvoie, base_urbain, eau_rpg_foret;


--
--/SCRIPT PARTIE 7 LACUNE
--

--Creation du MOS 
	BEGIN;
	SELECT msg('Start MOS');
	DROP TABLE IF EXISTS mos_int;
	CREATE TABLE mos_int 
	AS 
    		SELECT 'route'::text AS type, nature, geom_buf_s AS geom 
		FROM route_select;
		INSERT INTO mos_int  (
    			SELECT 'voies_ferre' AS type, nature, geom_buf_s AS geom
			FROM vf_select
		);
		INSERT INTO mos_int  (
    			SELECT 'surf_route' AS type, nature, geom
			FROM surf_route_select
		);

		INSERT INTO mos_int  (
    			SELECT 'eau' AS type, '' AS nature, geom 
			FROM eau
		);
		INSERT INTO mos_int (		 
			SELECT CASE 
				WHEN typ = 1 
				THEN 'champs' 
				WHEN typ = 2 
				THEN 'prairie' 
				WHEN typ = 3 
				THEN 'verger' 
				ELSE 'autre agri.' 
				END AS type, '' AS nature, geom 
			FROM rpg_final
		);
		INSERT INTO mos_int (
			SELECT 'foret' AS type,'' AS nature, geom 
			FROM foret_final
		);
		INSERT INTO mos_int (
			SELECT 'urbain' AS type,'' AS nature, geom 
			FROM urbain_final
		);
	
	CREATE INDEX sidx_mos_int ON mos_int USING gist (geom);
	ANALYZE mos_int;
	COMMIT;

	BEGIN;		
	SELECT msg('Start MOS1');  
	--AJOUT 5 Creation des bas cotes (pour ensemble non renseigne dans MOS intermediaire) bordant les axes majeur du résultat du MOS intermédiaire(EAU,foret,route,tache_urbain bd_topo et champ du rpg)
	DROP TABLE IF EXISTS  bas_cote;
	CREATE TABLE bas_cote 
	AS
		SELECT st_makevalid_poly((st_dump(st_diff_approx(t2.geom, st_union(
			CASE WHEN mos_int.geom IS null 
			THEN st_geomfromtext('GEOMETRYCOLLECTION EMPTY') 
			ELSE mos_int.geom END)))).geom) geom
		FROM 
		(	
			SELECT st_buffer(r.geom, (r.largeur/2::double precision)+prec) AS geom
			FROM route r, zone_buf z, param
			WHERE st_intersects(z.geom, r.geom) AND r.franchisst::text <> 'Tunnel'::text AND
			(nature ='Autoroute' OR nature ='Quasi-autoroute' OR nature ='Route à 2 chaussées')
		) t2 
		LEFT JOIN 
		mos_int 
		on st_intersects(t2.geom, mos_int.geom) and t2.geom && mos_int.geom
		GROUP BY t2.geom
		having st_area(t2.geom) > 1;
	
	CREATE INDEX sidx_bas_cote_geom ON bas_cote USING gist (geom);
	ANALYZE bas_cote;

	INSERT INTO mos_int  (
    		SELECT 'bas_cote' AS type,'' AS nature, geom 
		FROM bas_cote);

	DELETE FROM mos_int
 	WHERE geom IS null or (geometrytype(geom) <> 'POLYGON' and geometrytype(geom) <> 'MULTIPOLYGON');
	COMMIT;

	BEGIN;
	SELECT msg('Start MOS2');
	-- extract prairie pelouse et coupe sur la grille
	DROP TABLE IF EXISTS  mos_int_t;
	CREATE TABLE mos_int_t
	AS
		SELECT type, nature, st_force2d(geom) AS geom
		FROM mos_int;
		
	DROP TABLE IF EXISTS  mos_int;
	CREATE TABLE mos_int
	AS
		SELECT *
		FROM mos_int_t;
	CREATE INDEX sidx_mos_int_geom ON mos_int USING gist (geom);

		DROP TABLE IF EXISTS  clc_grid;
	CREATE TABLE clc_grid 
	AS
		SELECT grid.id, st_makevalid_poly(st_intersection(grid.geom, clc.geom)) geom
		FROM grid,clc_mos clc
		WHERE st_intersects(grid.geom, clc.geom) and (clc.code_06 = '321' or clc.code_06 = '231');
	CREATE INDEX sidx_clc_grid_geom ON clc_grid USING gist (geom);
	ANALYZE clc_grid;
	COMMIT;

	BEGIN;
	-- mos_int coupe sur la grille
	DROP TABLE IF EXISTS  mos_int_grid;
	CREATE TABLE mos_int_grid 
	AS
		SELECT grid.id, type, nature, st_makevalid_poly(st_intersection(grid.geom, mos_int.geom)) geom
		FROM grid,mos_int
		WHERE st_intersects(grid.geom, mos_int.geom) ;

	CREATE INDEX sidx_mos_int_grid_geom ON mos_int_grid USING gist (geom);
	ANALYZE mos_int_grid;
	COMMIT;

	BEGIN;
	SELECT msg('Start MOS3');
	--ajout des prairies et pelouse (code 321 et 231) du CLC dans mos_int_grid
	INSERT INTO mos_int_grid (		 
		SELECT id, 'pp' 
		AS type, '' AS nature, geom 
		FROM (
			SELECT clc.id, st_makevalid_poly(st_diff_approx(st_force2d(clc.geom), st_union_approx(st_collect(
				CASE WHEN mos_int.geom IS null 
				THEN st_geomfromtext('GEOMETRYCOLLECTION EMPTY') 
				ELSE mos_int.geom END)))) geom
			FROM clc_grid clc 
			LEFT JOIN 
			mos_int_grid mos_int 
			on clc.id = mos_int.id and st_intersects(clc.geom, mos_int.geom)
			GROUP BY clc.id, clc.geom
		) t
	);
	COMMIT;

	BEGIN;
		SELECT msg('Start MOS3');
	DROP TABLE IF EXISTS mos_int_grid_union;
	CREATE TABLE mos_int_grid_union 
	AS 
		SELECT id, type, nature, geom
		FROM (
			SELECT id, nature, type, st_makevalid_poly((st_dump(geom)).geom) geom
			FROM (
	   			SELECT id, nature,  type, st_union_approx(st_collect(geom)) geom
	   			FROM mos_int_grid
	    			GROUP BY id,nature, type
			) t
		) t1
		WHERE st_area(geom) > 1;

	CREATE INDEX sidx_mos_int_grid_union_geom ON mos_int_grid_union USING gist (geom);
	CREATE INDEX idx_mos_int_grid_union_id ON mos_int_grid_union USING btree (id);
	ANALYZE mos_int_grid_union;
	COMMIT;


DROP TABLE IF EXISTS mos_int, mos_int_grid, clc_grid, bas_cote;

	BEGIN;		  
	SELECT msg('Start MOS4 reste');
	--AJOUT 7 Tout le reste sur grid
	DROP TABLE IF EXISTS reste_grid;
	CREATE TABLE reste_grid 
	AS
		SELECT row_number() over () id, st_makevalid_poly(geom) geom
		FROM param, (
			SELECT (st_dump(geom)).geom geom
			FROM param, 
			(
				SELECT z.id, st_makevalid_poly(st_diff_approx(z.geom, 
				st_makevalid_poly(st_union_approx(st_collect(
					CASE WHEN mos.geom IS null 
					THEN st_geomfromtext('GEOMETRYCOLLECTION EMPTY') 
					ELSE mos.geom END
				))))) geom
				FROM grid z 
				LEFT JOIN 
				mos_int_grid_union mos 
				on z.id = mos.id
				GROUP BY z.id, z.geom
			) t
		) t1;
	
	ALTER TABLE reste_grid ADD CONSTRAINT reste_grid_pk PRIMARY KEY(id);
	CREATE INDEX sidx_reste_grid_geom ON reste_grid USING gist (geom);
	ANALYZE reste_grid;
	COMMIT;

	BEGIN;
	SELECT msg('Start big reste');
	DROP TABLE IF EXISTS big_reste;
	CREATE TABLE big_reste 
	AS
		SELECT * 
		FROM reste_grid
		WHERE st_area(geom) > 2000;
	
	ALTER TABLE big_reste ADD CONSTRAINT big_reste_pk PRIMARY KEY(id);
	CREATE INDEX sidx_big_reste_geom ON big_reste USING gist (geom);
	ANALYZE big_reste;

	DELETE FROM reste_grid
	WHERE st_area(geom) > 2000;
	COMMIT;

	BEGIN;
	SELECT msg('Start small reste');
	DROP TABLE IF EXISTS small_reste;
	CREATE TABLE small_reste 
	AS
		SELECT reste_grid.* 
		FROM reste_grid, grid
		WHERE ST_ContainsProperly(grid.geom, reste_grid.geom);

	ALTER TABLE small_reste ADD CONSTRAINT small_reste_pk PRIMARY KEY(id);
	CREATE INDEX sidx_small_reste_geom ON small_reste USING gist (geom);
	ANALYZE small_reste;

	DELETE FROM reste_grid
	WHERE id in (SELECT id FROM small_reste);
	COMMIT;



	---
	--- il faut boucler sur ce bloque tant qu'il y en a (Fonction très spécifique ajouter au début du script avec les autres fonctions mais rappelée ici)
	BEGIN;
	--	CREATE OR REPLACE FUNCTION merge_big_reste() returns void AS 
	--	$$
	--	BEGIN
	--    		FOUND := TRUE;
	--    		WHILE FOUND LOOP
	--    		PERFORM msg('Boucle reste');
	--    			INSERT INTO big_reste (
	--				SELECT DISTINCT reste_grid.* 
	--				FROM reste_grid, big_reste
	--				WHERE st_intersects(reste_grid.geom, big_reste.geom)
	--    			);
	--    			DELETE FROM reste_grid 
	--			WHERE id 
	--			in (SELECT id FROM big_reste);
	--    		END LOOP;
	--	END
	--	$$
	--	LANGUAGE 'plpgsql' VOLATILE STRICT;
	SELECT merge_big_reste();
	COMMIT;

	BEGIN;
	SELECT msg('Start grid_union');
	DROP TABLE IF EXISTS reste_grid_union;
	CREATE TABLE reste_grid_union 
	AS
		SELECT (st_dump(st_union(geom))).geom geom
		FROM reste_grid;

	INSERT INTO small_reste (
		SELECT (row_number() over ()) + (SELECT max(id) FROM small_reste) id, geom
		FROM reste_grid_union
		WHERE st_area(geom) <= 2000
	);
	INSERT INTO big_reste (
		SELECT (row_number() over ()) + (SELECT max(id) FROM big_reste) id, geom
		FROM reste_grid_union
		WHERE st_area(geom) > 2000
	);
	COMMIT;


	BEGIN;		  
	--AJOUT 7 
	
	SELECT msg('Start reste voisin');
	DROP TABLE IF EXISTS reste_voisin;
	CREATE TABLE reste_voisin 
	AS
	SELECT id, type, sum(
		CASE WHEN area > 0 
		THEN len/2 
		ELSE len 
		END) AS len, geom
	FROM (
		SELECT reste.id, mos.type, st_length(st_inter_approx(reste.geom, mos.geom)) len, 
		st_area(st_inter_approx(reste.geom, mos.geom)) area, reste.geom
		FROM small_reste reste 
		LEFT JOIN 
		mos_int_grid_union mos 
		on st_intersects(reste.geom, mos.geom)
	) t
	GROUP BY id, type, geom;
	CREATE INDEX sidx_reste_voisin_geom ON reste_voisin USING gist (geom);
	ANALYZE reste_voisin;
	COMMIT;

	BEGIN;		  
	--AJOUT 7 
	SELECT msg('Start reste voisin max ss 1/4');
	DROP TABLE IF EXISTS reste_voisin_max_t;
	CREATE TABLE reste_voisin_max_t 
	AS
			SELECT id, max(len) max_len, geom
			FROM reste_voisin 
			WHERE type <> 'voie' and type <> 'eau' 
			and type <> 'bas_cote' AND type <> 'surf_route' 
			AND type <> 'route' AND type<> 'vf'
			GROUP BY id, geom;
	ALTER TABLE reste_voisin_max_t ADD CONSTRAINT reste_voisin_max_t_pk PRIMARY KEY(id);

	SELECT msg('Start reste voisin max ss 2/4');
	DROP TABLE IF EXISTS reste_voisin_max_2;
	CREATE TABLE reste_voisin_max_2 
	AS
		SELECT reste_voisin.id, reste_voisin.type, 
		reste_voisin.geom, reste_voisin.len, reste_max.max_len 
		FROM reste_voisin, reste_voisin_max_t reste_max
		WHERE reste_voisin.id = reste_max.id;

	SELECT msg('Start reste voisin max ss 3/4');
	DROP TABLE IF EXISTS reste_voisin_max_3;
	CREATE TABLE reste_voisin_max_3
	AS
		SELECT reste.id, reste.type, reste.geom
		FROM reste_voisin_max_2 reste
		WHERE reste.len = reste.max_len;

	SELECT msg('Start reste voisin max ss 4/4');
	DROP TABLE IF EXISTS reste_voisin_max;
	CREATE TABLE reste_voisin_max 
	AS
		SELECT reste.id, MIN(reste.type) AS type, reste.geom
		FROM reste_voisin_max_3 reste
		GROUP BY reste.id, reste.geom;
	ALTER TABLE reste_voisin_max ADD CONSTRAINT reste_voisin_max_pk PRIMARY KEY(id);
	CREATE INDEX sidx_reste_voisin_max_geom ON reste_voisin_max USING gist (geom);
	ANALYZE reste_voisin_max;
	COMMIT;

	BEGIN;	
	SELECT msg('Start int_grid_union');
	INSERT INTO mos_int_grid_union  (
		SELECT grid.id, type,'' AS nature, st_clean_poly(st_intersection(reste.geom, grid.geom),snap) geom
		FROM reste_voisin_max reste, grid, param
		WHERE st_intersects(reste.geom, grid.geom)
	);
	DELETE FROM reste_voisin
	USING reste_voisin_max
	WHERE reste_voisin_max.id = reste_voisin.id;
	COMMIT;


	BEGIN;
	--AJOUT 7 
	
	SELECT msg('Start reste voisin max');
	DROP TABLE IF EXISTS reste_voisin_max;
	CREATE TABLE reste_voisin_max 
	AS
		SELECT reste_voisin.id, MIN(reste_voisin.type) AS type, reste_voisin.geom
		FROM reste_voisin,
		(
			SELECT id, max(len) max_len
			FROM reste_voisin 
			GROUP BY id
		) reste_max
		WHERE reste_voisin.id = reste_max.id and reste_voisin.len = reste_max.max_len
		GROUP BY reste_voisin.id, reste_voisin.geom;

	CREATE INDEX sidx_reste_voisin_max_geom ON reste_voisin_max USING gist (geom);
	ANALYZE reste_voisin_max;

	INSERT INTO mos_int_grid_union  (
		SELECT grid.id, type,'' AS nature, st_clean_poly(st_intersection(reste.geom, grid.geom),snap) geom
		FROM reste_voisin_max reste, grid, param
		WHERE st_intersects(reste.geom, grid.geom)
	);

	DELETE FROM reste_voisin
	USING reste_voisin_max
	WHERE reste_voisin_max.id = reste_voisin.id;
	COMMIT;

--
-- GESTION D'erreur 
-- Partie redondante mais nécessaire sur les polygone de reste voisin n'ayant pas été attribué car leur géométrie à été altérée de 0.0001m -> polygone non valide ayant nécessité l'intervention d'approximation
SELECT msg('gestion erreur');
DROP TABLE IF EXISTS reste_voisin_e;
	CREATE TABLE reste_voisin_e
	AS
		SELECT id, type, len, st_buffer(geom, approx) geom
		FROM reste_voisin, param;
	CREATE INDEX sidx_reste_voisin_e_geom ON reste_voisin_e USING gist (geom);

SELECT msg('Start reste voisin');
	DROP TABLE IF EXISTS reste_voisin;
	CREATE TABLE reste_voisin 
	AS
	SELECT id, type, sum(
		CASE WHEN area > 0 
		THEN len/2 
		ELSE len 
		END) AS len, geom
	FROM (
		SELECT reste.id, mos.type, st_length(st_inter_approx(reste.geom, mos.geom)) len, 
		st_area(st_inter_approx(reste.geom, mos.geom)) area, reste.geom
		FROM reste_voisin_e reste 
		LEFT JOIN 
		mos_int_grid_union mos 
		on st_intersects(reste.geom, mos.geom)
	) t
	GROUP BY id, type, geom;
	CREATE INDEX sidx_reste_voisin_geom ON reste_voisin USING gist (geom);
	ANALYZE reste_voisin;
	COMMIT;

	BEGIN;		  
	--AJOUT 7 
	SELECT msg('Start reste voisin max ss 1/4');
	DROP TABLE IF EXISTS reste_voisin_max_t;
	CREATE TABLE reste_voisin_max_t 
	AS
			SELECT id, max(len) max_len, geom
			FROM reste_voisin 
			WHERE type <> 'voie' and type <> 'eau' 
			and type <> 'bas_cote' AND type <> 'surf_route' 
			AND type <> 'route' AND type<> 'vf'
			GROUP BY id, geom;
	ALTER TABLE reste_voisin_max_t ADD CONSTRAINT reste_voisin_max_t_pk PRIMARY KEY(id);

	SELECT msg('Start reste voisin max ss 2/4');
	DROP TABLE IF EXISTS reste_voisin_max_2;
	CREATE TABLE reste_voisin_max_2 
	AS
		SELECT reste_voisin.id, reste_voisin.type, 
		reste_voisin.geom, reste_voisin.len, reste_max.max_len 
		FROM reste_voisin, reste_voisin_max_t reste_max
		WHERE reste_voisin.id = reste_max.id;

	SELECT msg('Start reste voisin max ss 3/4');
	DROP TABLE IF EXISTS reste_voisin_max_3;
	CREATE TABLE reste_voisin_max_3
	AS
		SELECT reste.id, reste.type, reste.geom
		FROM reste_voisin_max_2 reste
		WHERE reste.len = reste.max_len;

	SELECT msg('Start reste voisin max ss 4/4');
	DROP TABLE IF EXISTS reste_voisin_max;
	CREATE TABLE reste_voisin_max 
	AS
		SELECT reste.id, MIN(reste.type) AS type, reste.geom
		FROM reste_voisin_max_3 reste
		GROUP BY reste.id, reste.geom;
	ALTER TABLE reste_voisin_max ADD CONSTRAINT reste_voisin_max_pk PRIMARY KEY(id);
	CREATE INDEX sidx_reste_voisin_max_geom ON reste_voisin_max USING gist (geom);
	ANALYZE reste_voisin_max;
	COMMIT;

	BEGIN;	
	SELECT msg('Start int_grid_union');
	INSERT INTO mos_int_grid_union  (
		SELECT grid.id, type,'' AS nature, st_clean_poly(st_intersection(reste.geom, grid.geom),snap) geom
		FROM reste_voisin_max reste, grid, param
		WHERE st_intersects(reste.geom, grid.geom)
	);
	DELETE FROM reste_voisin
	USING reste_voisin_max
	WHERE reste_voisin_max.id = reste_voisin.id;
	COMMIT;


	BEGIN;
	--AJOUT 7 
	
	SELECT msg('Start reste voisin max');
	DROP TABLE IF EXISTS reste_voisin_max;
	CREATE TABLE reste_voisin_max 
	AS
		SELECT reste_voisin.id, MIN(reste_voisin.type) AS type, reste_voisin.geom
		FROM reste_voisin,
		(
			SELECT id, max(len) max_len
			FROM reste_voisin 
			GROUP BY id
		) reste_max
		WHERE reste_voisin.id = reste_max.id and reste_voisin.len = reste_max.max_len
		GROUP BY reste_voisin.id, reste_voisin.geom;

	CREATE INDEX sidx_reste_voisin_max_geom ON reste_voisin_max USING gist (geom);
	ANALYZE reste_voisin_max;

	INSERT INTO mos_int_grid_union  (
		SELECT grid.id, type,'' AS nature, st_clean_poly(st_intersection(reste.geom, grid.geom),snap) geom
		FROM reste_voisin_max reste, grid, param
		WHERE st_intersects(reste.geom, grid.geom)
	);

	DELETE FROM reste_voisin
	USING reste_voisin_max
	WHERE reste_voisin_max.id = reste_voisin.id;
	COMMIT;
--
-- Fin de la gestion d'erreur


	BEGIN;
	
	SELECT msg('Start mos_intn');
	DROP TABLE IF EXISTS mos_int_grid;
	CREATE TABLE mos_int_grid 
	AS 
	SELECT id, type, nature, (st_dump(geom)).geom geom
	FROM (
    		SELECT max(mos.id) id, type, nature,
		st_makevalid_poly(st_union_approx(st_collect(mos.geom))) geom
    		FROM mos_int_grid_union mos lEFT JOIN grid
		ON st_within(mos.geom, grid.geom)
    		GROUP BY nature, type, grid.id
	) t;
	COMMIT;
	BEGIN;

	INSERT INTO mos_int_grid (
		SELECT (max(mos.id)+br.id-min(br.id)) id, 'reste' AS type, '' AS nature, br.geom
		FROM big_reste br, mos_int_grid_union mos
		GROUP BY br.id
	);

	CREATE INDEX sidx_mos_int_grid_geom ON mos_int_grid USING gist (geom);
	ANALYZE mos_int_grid;
	COMMIT;



--
-- GESTION d'élément inférieur à la limite mais possédant un attribut
-- 
	DROP TABLE IF EXISTS reclassement;
	CREATE TABLE reclassement
	AS
		SELECT * FROM mos_int_grid
		WHERE ST_area(geom) < 500 AND type <> 'voie' and type <> 'eau' and type <> 'bas_cote' 
			AND type <> 'surf_route' AND type <> 'route' AND type<> 'vf'
			AND type <> 'reste';
	CREATE INDEX sidx_reclassement_geom ON reclassement USING gist (geom);

	DELETE FROM mos_int_grid
	USING reclassement
	WHERE reclassement.id = mos_int_grid.id;
	COMMIT;

SELECT msg('Start reste voisin');
	DROP TABLE IF EXISTS reste_voisin;
	CREATE TABLE reste_voisin 
	AS
	SELECT id, type, sum(
		CASE WHEN area > 0 
		THEN len/2 
		ELSE len 
		END) AS len, geom
	FROM (
		SELECT reste.id, mos.type, st_length(st_inter_approx(reste.geom, mos.geom)) len, 
		st_area(st_inter_approx(reste.geom, mos.geom)) area, reste.geom
		FROM 
		reclassement reste 
		LEFT JOIN 
		mos_int_grid_union mos 
		on st_intersects(reste.geom, mos.geom)
	) t
	GROUP BY id, type, geom;
	CREATE INDEX sidx_reste_voisin_geom ON reste_voisin USING gist (geom);
	ANALYZE reste_voisin;
	COMMIT;

	BEGIN;		  
	--AJOUT 7 
	SELECT msg('Start reste voisin max ss 1/4');
	DROP TABLE IF EXISTS reste_voisin_max_t;
	CREATE TABLE reste_voisin_max_t 
	AS
			SELECT id, max(len) max_len, geom
			FROM reste_voisin 
			WHERE type <> 'voie' and type <> 'eau' 
			and type <> 'bas_cote' AND type <> 'surf_route' 
			AND type <> 'route' AND type<> 'vf'
			GROUP BY id, geom;

	SELECT msg('Start reste voisin max ss 2/4');
	DROP TABLE IF EXISTS reste_voisin_max_2;
	CREATE TABLE reste_voisin_max_2 
	AS
		SELECT reste_voisin.id, reste_voisin.type, 
		reste_voisin.geom, reste_voisin.len, reste_max.max_len 
		FROM reste_voisin, reste_voisin_max_t reste_max
		WHERE reste_voisin.id = reste_max.id;

	SELECT msg('Start reste voisin max ss 3/4');
	DROP TABLE IF EXISTS reste_voisin_max_3;
	CREATE TABLE reste_voisin_max_3
	AS
		SELECT reste.id, reste.type, reste.geom
		FROM reste_voisin_max_2 reste
		WHERE reste.len = reste.max_len;

	SELECT msg('Start reste voisin max ss 4/4');
	DROP TABLE IF EXISTS reste_voisin_max;
	CREATE TABLE reste_voisin_max 
	AS
		SELECT reste.id, MIN(reste.type) AS type, reste.geom
		FROM reste_voisin_max_3 reste
		GROUP BY reste.id, reste.geom;
	CREATE INDEX sidx_reste_voisin_max_geom ON reste_voisin_max USING gist (geom);
	ANALYZE reste_voisin_max;
	COMMIT;

	BEGIN;	
	SELECT msg('Start int_grid_union');
	INSERT INTO mos_int_grid_union  (
		SELECT grid.id, type,'' AS nature, st_clean_poly(st_intersection(reste.geom, grid.geom),snap) geom
		FROM reste_voisin_max reste, grid, param
		WHERE st_intersects(reste.geom, grid.geom)
	);
	DELETE FROM reste_voisin
	USING reste_voisin_max
	WHERE reste_voisin_max.id = reste_voisin.id;
	COMMIT;


	BEGIN;
	--AJOUT 7 

	SELECT msg('Start reste voisin max');
	DROP TABLE IF EXISTS reste_voisin_max;
	CREATE TABLE reste_voisin_max 
	AS
		SELECT reste_voisin.id, MIN(reste_voisin.type) AS type, reste_voisin.geom
		FROM reste_voisin,
		(
			SELECT id, max(len) max_len
			FROM reste_voisin 
			GROUP BY id
		) reste_max
		WHERE reste_voisin.id = reste_max.id and reste_voisin.len = reste_max.max_len
		GROUP BY reste_voisin.id, reste_voisin.geom;

	CREATE INDEX sidx_reste_voisin_max_geom ON reste_voisin_max USING gist (geom);
	ANALYZE reste_voisin_max;
	
	SELECT msg('Start reste voisin max');
	INSERT INTO mos_int_grid_union  (
		SELECT grid.id, type, st_clean_poly(st_intersection(reste.geom, grid.geom),snap) geom
		FROM reste_voisin_max reste, grid, param
		WHERE st_intersects(reste.geom, grid.geom)
	);
	
	SELECT msg('Start reste voisin max');
	DELETE FROM reste_voisin
	USING reste_voisin_max
	WHERE reste_voisin_max.id = reste_voisin.id;
	COMMIT;
--
-- Fin de la gestion d'erreur

	BEGIN;
	SELECT msg('finalisation_big_reste');
	INSERT INTO mos_int_grid_union (
		SELECT 999 as id, '' as type, '' as nature, st_clean_poly(big_reste.geom, snap) geom
		FROM big_reste, param
		);
	
	SELECT msg('Dernière étape');
	CREATE TABLE mos_final AS
		SELECT * FROM mos_int_grid_union;
	SELECT msg('FIN');
	COMMIT;

	--Nettoyage des couches obsoletes du schéma
	DROP TABLE IF EXISTS big_reste;
	DROP TABLE IF EXISTS df12;
	DROP TABLE IF EXISTS eau_int;
	DROP TABLE IF EXISTS eau_route;
	DROP TABLE IF EXISTS foret_colle;
	DROP TABLE IF EXISTS grid;
	DROP TABLE IF EXISTS grid4;
	DROP TABLE IF EXISTS mos_int_grid;
	DROP TABLE IF EXISTS mos_int_grid_union;
	DROP TABLE IF EXISTS mos_int_t;
	DROP TABLE IF EXISTS n_foret;
	DROP TABLE IF EXISTS nt_rpg;
	DROP TABLE IF EXISTS nt_rpg_t;
	DROP TABLE IF EXISTS nt_rpg_t2;
	DROP TABLE IF EXISTS oignon_2;
	DROP TABLE IF EXISTS oignon_3;
	DROP TABLE IF EXISTS oignon_4;
	DROP TABLE IF EXISTS oignon_5;
	DROP TABLE IF EXISTS r_t;
	DROP TABLE IF EXISTS reclassement;
	DROP TABLE IF EXISTS reste_grid;
	DROP TABLE IF EXISTS reste_grid_union;
	DROP TABLE IF EXISTS reste_voisin;
	DROP TABLE IF EXISTS reste_voisin_e;
	DROP TABLE IF EXISTS reste_voisin_max;
	DROP TABLE IF EXISTS reste_voisin_max_2;
	DROP TABLE IF EXISTS reste_voisin_max_3;
	DROP TABLE IF EXISTS reste_voisin_max_t;
	DROP TABLE IF EXISTS route_vf_sr;
	DROP TABLE IF EXISTS rpg5;
	DROP TABLE IF EXISTS rpg_buf_oignon;
	DROP TABLE IF EXISTS rpg_final_t;
	DROP TABLE IF EXISTS rpg_final_t2;
	DROP TABLE IF EXISTS rpg_int_oignon;
	DROP TABLE IF EXISTS rpg_inter;
	DROP TABLE IF EXISTS rpg_left_right;
	DROP TABLE IF EXISTS rpg_lr;
	DROP TABLE IF EXISTS rpg_right;
	DROP TABLE IF EXISTS rpg_t;
	DROP TABLE IF EXISTS rpg_typ;
	DROP TABLE IF EXISTS rpg_typ_ssvoie;
	DROP TABLE IF EXISTS select_tot_vf_r;
	DROP TABLE IF EXISTS small_reste;
	DROP TABLE IF EXISTS urbain_ssvoie;
	DROP TABLE IF EXISTS urbain_ssvoie_t;
	DROP TABLE IF EXISTS urbain_ssvoie_t2;
	DROP TABLE IF EXISTS vege2;
	DROP TABLE IF EXISTS vege_ss_err;
	DROP TABLE IF EXISTS voie_buf_0;
	DROP TABLE IF EXISTS voie_buf_1;
	DROP TABLE IF EXISTS zone5;
	DROP TABLE IF EXISTS rpg_left;
	DROP TABLE IF EXISTS rpg_mos;
	DROP TABLE IF EXISTS clc_mos;
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
