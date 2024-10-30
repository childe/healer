package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
)

var describeAclsCmd = &cobra.Command{
	Use:   "describe-acls",
	Short: "describe acls",

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

		req := healer.DescribeAclsRequestBody{}

		// 处理所有标志
		resourcetype, _ := cmd.Flags().GetString("resourcetype")
		json.Unmarshal([]byte(`"`+resourcetype+`"`), &req.ResourceType)

		resource, _ := cmd.Flags().GetString("resource")
		json.Unmarshal([]byte(`"`+resource+`"`), &req.ResourceName)

		if resourcepatterntype, err := cmd.Flags().GetString("patterntype"); err != nil {
			return err
		} else {
			json.Unmarshal([]byte(`"`+resourcepatterntype+`"`), &req.PatternType)
		}

		req.Principal, _ = cmd.Flags().GetString("principal")

		req.Host, _ = cmd.Flags().GetString("host")

		operation, _ := cmd.Flags().GetString("operation")
		json.Unmarshal([]byte(`"`+operation+`"`), &req.Operation)

		permissiontype, _ := cmd.Flags().GetString("permissiontype")
		json.Unmarshal([]byte(`"`+permissiontype+`"`), &req.PermissionType)

		responses, err := admin.DescribeAcls(req)
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
	describeAclsCmd.Flags().String("resourcetype", "", "resourcetype: any|topic|group|cluster")
	describeAclsCmd.Flags().String("resource", "", "resource")
	describeAclsCmd.Flags().String("patterntype", "", "patterntype: any|match|literal|prefixed")
	describeAclsCmd.Flags().String("principal", "", "principal")
	describeAclsCmd.Flags().String("host", "", "host")
	describeAclsCmd.Flags().String("operation", "", "operation: any|read|write|create|delete|alter|describe|clusteraction|describeconfigs|alterconfigs|idempotentwrite")
	describeAclsCmd.Flags().String("permissiontype", "", "permissiontype: any|allow|deny")

	rootCmd.AddCommand(describeAclsCmd)
}
