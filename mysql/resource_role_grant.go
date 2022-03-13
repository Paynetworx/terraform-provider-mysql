package mysql

import (
	"fmt"
	"database/sql"
	"strings"
	"regexp"
	"log"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)


func resourceRoleGrant() *schema.Resource {
	return &schema.Resource{
		Create: CreateRoleGrant,
		Read:   ReadRoleGrant,
		Delete: DeleteRoleGrant,

		Schema: map[string]*schema.Schema{
			"user": {
				Type:          schema.TypeString,
				ForceNew:      true,
				Required:	   true,
			},

			"role": {
				Type:          schema.TypeString,
				Required:	   true,
				ForceNew:      true,
			},

			"host": {
				Type:          schema.TypeString,
				Required:	   true,
				ForceNew:      true,
			},
		},
	}
}

func CreateRoleGrant(d *schema.ResourceData, meta interface{}) error {
	db, err := meta.(*MySQLConfiguration).GetDbConn()
	if err != nil {
		return err
	}

	user := d.Get("user").(string)
	host := d.Get("host").(string)
	role := d.Get("role").(string)

	stmtSQL := fmt.Sprintf("GRANT '%s' TO '%s'@'%s'", role, user, host)

	log.Println("Executing statement:", stmtSQL)
	_, err = db.Exec(stmtSQL)
	if err != nil {
		return fmt.Errorf("Error running SQL (%s): %s", stmtSQL, err)
	}

	id := fmt.Sprintf("%s@%s:%s", user, host, role)

	d.SetId(id)
	return ReadGrant(d, meta)
}

func DeleteRoleGrant(d *schema.ResourceData, meta interface{}) error {
	db, err := meta.(*MySQLConfiguration).GetDbConn()
	if err != nil {
		return err
	}

	user := d.Get("user").(string)
	host := d.Get("host").(string)
	role := d.Get("role").(string)


	var sql string
	sql = fmt.Sprintf("REVOKE '%s' FROM '%s'@'%s'", role, user, host)
	log.Printf("[DEBUG] SQL: %s", sql)
	_, err = db.Exec(sql)
	if err != nil {
		return fmt.Errorf("error revoking (%s): %s", sql, err)
	}

	return nil
}

func ReadRoleGrant(d *schema.ResourceData, meta interface{}) error {
	db, err := meta.(*MySQLConfiguration).GetDbConn()
	if err != nil {
		return err
	}

	user := d.Get("user").(string)
	host := d.Get("host").(string)
	role := d.Get("role").(string)

	id := fmt.Sprintf("%s@%s:%s", user, host, role)


	stmtSQL := fmt.Sprintf("SHOW GRANTS FOR '%s'", user)
	rows, err := db.Query(stmtSQL)

	if err != nil {
		return err
	}

	defer rows.Close()

	re := regexp.MustCompile(`^GRANT (.+) TO`)
	found := false

	for rows.Next() {
		var rawGrant string

		err := rows.Scan(&rawGrant)

		if err != nil {
			return err
		}

		m := re.FindStringSubmatch(rawGrant)

		if len(m) == 2 {
			role_name := strings.Trim(m[1],"'")
			if( role_name  == role ){
				found = true
			}
		}

	}

	if(found){
		d.SetId(id)
	}else{
		d.SetId("")
	}
	return nil
}
