package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
)

var deleteAclsCmd = &cobra.Command{
	Use:   "delete-acls",
	Short: "delete acls",

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

		f := &healer.DeleteAclsFilter{}

		// 处理所有标志
		resourcetype, _ := cmd.Flags().GetString("resourcetype")
		json.Unmarshal([]byte(`"`+resourcetype+`"`), &f.ResourceType)

		if cmd.Flags().Changed("resource") {
			resource, _ := cmd.Flags().GetString("resource")
			json.Unmarshal([]byte(`"`+resource+`"`), &f.ResourceName)
		}

		if resourcepatterntype, err := cmd.Flags().GetString("patterntype"); err != nil {
			return err
		} else {
			json.Unmarshal([]byte(`"`+resourcepatterntype+`"`), &f.PatternType)
		}

		if !cmd.Flags().Changed("principal") {
			f.Principal = nil
		} else {
			if principal, err := cmd.Flags().GetString("principal"); err != nil {
				return err
			} else {
				f.Principal = &principal
			}
		}

		if !cmd.Flags().Changed("host") {
			f.Host = nil
		} else {
			if host, err := cmd.Flags().GetString("host"); err != nil {
				return err
			} else {
				f.Host = &host
			}
		}

		operation, _ := cmd.Flags().GetString("operation")
		json.Unmarshal([]byte(`"`+operation+`"`), &f.Operation)

		permissiontype, _ := cmd.Flags().GetString("permissiontype")
		json.Unmarshal([]byte(`"`+permissiontype+`"`), &f.PermissionType)

		responses, err := admin.DeleteAcls([]*healer.DeleteAclsFilter{f})
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
	deleteAclsCmd.Flags().String("resourcetype", "any", "resourcetype: any|topic|group|cluster")
	deleteAclsCmd.Flags().String("resource", "", "resource")
	deleteAclsCmd.Flags().String("patterntype", "match", "patterntype: any|match|literal|prefixed")
	deleteAclsCmd.Flags().String("principal", "", "principal")
	deleteAclsCmd.Flags().String("host", "", "host")
	deleteAclsCmd.Flags().String("operation", "any", "operation: any|read|write|create|delete|alter|describe|clusteraction|describeconfigs|alterconfigs|idempotentwrite")
	deleteAclsCmd.Flags().String("permissiontype", "any", "permissiontype: any|allow|deny")

	rootCmd.AddCommand(deleteAclsCmd)
}
