DELIMITER $$
CREATE DEFINER=`root`@`%` PROCEDURE `sp_import_process_d_trial`(
in p_user_id INT,
in p_import_id INT,
in p_warehouse_id INT,
in p_company_id INT,
in p_account_id INT,
in p_user_name	varchar(100),
IN p_inbound_type_id INT ,
IN p_outbound_type_id INT
)
BEGIN

DECLARE v_column TEXT;
DECLARE v_left_join TEXT;
DECLARE v_valid TEXt;

DROP TEMPORARY TABLE IF EXISTS wms_utilities_dev.tmp_piv;
create temporary table wms_utilities_dev.tmp_piv
select  id.id,id.row_no,id.value,ifs.                                   ****CENSORED****             code FROM
wms_utilities_dev.import_dtl id

--INNER JOIN import_dtl id ON  ih.id=id.import_hdr_id 
inner join  wms_utilities_dev.import_mapping im ON im.id=id.import_mapping_id
INNER JOIN wms_utilities_dev.import_fields ifs ON ifs.sysfield=im.import_fieldname AND ifs.module_id=v_module_id 
#set id.is_valid=0,id.error_msg=concat(ifnull(id.error_msg,''),' Wrong Datatype')                                            ****Validation****
where id.import_hdr_id=p_import_id and account_id=p_account_id order by row_no,seq_no;

DROP TEMPORARY TABLE IF EXISTS wms_utilities_dev.tt_tmp_piv;
		create temporary table wms_utilities_dev.tt_tmp_piv
		select * from wms_utilities_dev.tmp_piv;

		SELECT
		  GROUP_CONCAT(DISTINCT
			CONCAT(
			  'max(case when import_fieldname = ''',
			  import_fieldname,
			  ''' then value end) `',
			  import_fieldname,'`'
			)
		  ) INTO @sql
		FROM
		  wms_utilities_dev.tt_tmp_piv;
          
		  DROP TEMPORARY TABLE IF EXISTS wms_utilities_dev.testdat;
		  SET @sql = CONCAT('create temporary table wms_utilities_dev.testdat SELECT A.row_no, ', @sql, ' 
						  , er.error_msg FROM wms_utilities_dev.tt_tmp_piv A',' left join( Select row_no, GROUP_CONCAT(error_msg) error_msg from (Select distinct row_no,error_msg from wms_utilities_dev.tmp_piv)A group by row_no)er on A.row_no=er.row_no','
						   GROUP BY A.row_no');

		PREPARE stmt FROM @sql;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
       SET v_group_col = (
  CASE 
    WHEN p_inbound_type_id = 0 THEN 'Supplier Name'
    WHEN p_inbound_type_id = 1 THEN 'Warehouse Code' 
    WHEN p_inbound_type_id = 2 THEN 'Customer Name'
  END
);
      

                                                                                                  ****CENSORED****
        
        BLOCK1 : BEGIN                                         --Declaration of CURSOR to store id of new added products to add for reference into another table for easy joining on the basis of ID.
                    DECLARE cur1 CURSOR 
                    FOR SELECT *
                    FROM testdat_da T;
                    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
			
					OPEN cur1;
					read_loop1: LOOP
					FETCH cur1 INTO v_supllier_code;
					-- end of loop statement
					IF done = 1 THEN
						LEAVE read_loop1;
					END IF;
                    Select v_supllier_code;
        BLOCK2 : BEGIN
					
					DECLARE cur2 CURSOR FOR
					SELECT DISTINCT seq_no
					FROM wms_utilities_dev.mst_import_tables_d where module_id = v_module_id;
					-- Declare a continue handler to exit the loop
					DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
			
					OPEN cur2;
					read_loop: LOOP
					FETCH cur2 INTO seq_no_val;
					-- end of loop statement
					IF done = 1 THEN
						LEAVE read_loop;
					END IF;

				-- Generate the dynamic SQL for the current seq_no
						SET @sql = NULL;
						SELECT
						GROUP_CONCAT(DISTINCT
						CONCAT(
						column_name
						)
					) INTO @sql
					FROM
					wms_utilities_dev.mst_import_tables_d where seq_no=seq_no_val 
					and module_id = v_module_id; -- and module_id=p_module_id to be added in paramenter;
                --  select @sql;

                                                                                                    ****CENSORED****
                    
                    #GETTING ALL COLUMNS OF TESTDAT
                    SET @testdat_columns = '';
					SELECT GROUP_CONCAT(column_name) INTO @testdat_columns 
					FROM information_schema.columns 
					WHERE TABLE_SCHEMA = 'wms_utilities_dev' and
					table_name = 'testdat2nd';
                  
                    #DYNAMIC SELECT CLAUSE
                    SET @select_clause = '';
                    select
					  GROUP_CONCAT(
						 IF(FIND_IN_SET(m.sysfield, @testdat_columns) > 0,
							CONCAT('t.`', m.sysfield, '`'),
							IFNULL(CONCAT(m.value, ' ', m.column_name),
								   CONCAT('NULL AS ', m.column_name))
						 )
						 ORDER BY FIND_IN_SET(m.column_name, @sql)
					) INTO @select_clause
					FROM mst_import_tables_d m
						WHERE m.seq_no = seq_no_val
						  AND module_id = v_module_id;
						 -- AND m.account_id = p_account_id;

                                                                                                        ****CENSORED****
                  
-- INSERT STATMENT FOR RESPECTIVE TABLES, GETTING DATA FROM TESTDAT
SET @query = CONCAT('INSERT INTO ', 
                    (SELECT table_name 
                     FROM wms_utilities_dev.mst_import_tables_d ms
                     WHERE ms.seq_no = seq_no_val group by table_name), '(', @sql, ')
SELECT ', @select_clause, '  
FROM wms_utilities_dev.testdat t \n');

-- CONCATING JOIN QUERIES AT THE END TO HANDLE ';' AT THE END OF QUERY
IF @join_clause IS NOT NULL THEN
  SET @query = CONCAT(@query, ' ', @join_clause);  
ELSE
  SET @query = CONCAT(@query, ';'); 
END IF;

-- GENERAL FOR SUPPLIER TYPE INBOUND
SET @query= replace(@query,'p_account_id',p_account_id);
SET @query= replace(@query,'v_company_id',v_company_id);

IF p_inbound_type_id = 1 THEN    -- WAREHOUSE RETURN
BEGIN
SET @query= replace(@query,'ms.account_id','ms.company_id');
SET @query= replace(@query,'`Supplier Name`','`Warehouse Code`');
SET @query= replace(@query,'box_per_invoice,po_no.`PO No`','box_per_invoice,`Warehouse Code`');
END;
END IF;

IF p_inbound_type_id = 2 THEN   -- SALES RETURN
BEGIN
-- ADD REPLACE STATEMENT AS REQUIRED
END;
END IF;



PREPARE stmt FROM @query;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
Select  @query;

#SEQ NUMBER CUR
END LOOP read_loop;
					END BLOCK2;                    
 #SUPPLIER NAME CUR                   
END LOOP read_loop1;
					END BLOCK1;
        
        END;
        END IF;

END$$
DELIMITER ;
