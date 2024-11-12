package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
)

var createAclsCmd = &cobra.Command{
	Use:   "create-acls",
	Short: "create acls",

	RunE: func(cmd *cobra.Command, args []string) error {
		brokers, err := cmd.Flags().GetString("brokers")
		if err != nil {
			return err
		}
		clientID, err := cmd.Flags().GetString("client")
		if err != nil {
			return err
		}

		admin, err := healer.NewClient(brokers, clientID)

		if err != nil {
			return err
		}

		aclCreation := healer.AclCreation{}

		resourcetype, _ := cmd.Flags().GetString("resourcetype")
		json.Unmarshal([]byte(`"`+resourcetype+`"`), &aclCreation.ResourceType)

		if cmd.Flags().Changed("resource") {
			resource, _ := cmd.Flags().GetString("resource")
			json.Unmarshal([]byte(`"`+resource+`"`), &aclCreation.ResourceName)
		}

		if resourcepatterntype, err := cmd.Flags().GetString("patterntype"); err != nil {
			return err
		} else {
			json.Unmarshal([]byte(`"`+resourcepatterntype+`"`), &aclCreation.PatternType)
		}

		if aclCreation.Principal, err = cmd.Flags().GetString("principal"); err != nil {
			return err
		}

		if aclCreation.Host, err = cmd.Flags().GetString("host"); err != nil {
			return err
		}

		operation, _ := cmd.Flags().GetString("operation")
		json.Unmarshal([]byte(`"`+operation+`"`), &aclCreation.Operation)

		permissiontype, _ := cmd.Flags().GetString("permissiontype")
		json.Unmarshal([]byte(`"`+permissiontype+`"`), &aclCreation.PermissionType)

		responses, err := admin.CreateAcls([]healer.AclCreation{aclCreation})
		if err != nil {
			return err
		}

		s, err := json.MarshalIndent(responses, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(s))

		return nil
	},
}

func init() {
	createAclsCmd.Flags().String("resourcetype", "any", "resourcetype: any|topic|group|cluster")
	createAclsCmd.Flags().String("resource", "", "resource")
	createAclsCmd.Flags().String("patterntype", "match", "patterntype: any|match|literal|prefixed")
	createAclsCmd.Flags().String("principal", "", "principal")
	createAclsCmd.Flags().String("host", "", "host")
	createAclsCmd.Flags().String("operation", "any", "operation: any|read|write|create|delete|alter|describe|clusteraction|describeconfigs|alterconfigs|idempotentwrite")
	createAclsCmd.Flags().String("permissiontype", "any", "permissiontype: any|allow|deny")

	rootCmd.AddCommand(createAclsCmd)
}
