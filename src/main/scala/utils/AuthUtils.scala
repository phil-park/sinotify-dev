package utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

object AuthUtils{

  def authenticate(principal: String, keytabPath: String): UserGroupInformation ={
    val conf = new Configuration()
    conf.set("hadoop.security.authentication", "Kerberos")
    UserGroupInformation.setConfiguration(conf)
    if (UserGroupInformation.isSecurityEnabled) {
      UserGroupInformation.loginUserFromKeytab(principal, keytabPath)
    }
    UserGroupInformation.getLoginUser
  }
}
