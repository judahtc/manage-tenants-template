def activate_user_html(url: str, qrcode_image):
    html_body = f"""<!DOCTYPE html>
            <html lang="en-US">

            <head>
                <meta charset="utf-8">
                <title>Reset Password Email Template</title>
                <meta name="description" content="Reset Password Email Template.">
                <style type="text/css">

                </style>
            </head>

            <body style="margin: 0px; background-color: #f2f3f8;">
                <table cellspacing="0" border="0" cellpadding="0" width="100%" bgcolor="#f2f3f8"
                    style="@import url(https://fonts.googleapis.com/css?family=Rubik:300,400,500,700|Open+Sans:300,400,600,700); font-family: 'Open Sans', sans-serif;">
                    <tr>
                        <td>
                            <table style="background-color: #f2f3f8; max-width:670px; margin:0 auto;" width="100%" border="0"
                                align="center" cellpadding="0" cellspacing="0">
                                <tr>
                                    <td style="height:80px;">&nbsp;</td>
                                </tr>
                                <tr>
                                    <td style="text-align:center;">
                                    </td>
                                </tr>
                                <tr>
                                    <td style="height:20px;">&nbsp;</td>
                                </tr>
                                <tr>
                                    <td>
                                        <table width="95%" border="0" align="center" cellpadding="0" cellspacing="0"
                                            style="max-width:670px;background:#fff; border-radius:3px; text-align:center;-webkit-box-shadow:0 6px 18px 0 rgba(0,0,0,.06);-moz-box-shadow:0 6px 18px 0 rgba(0,0,0,.06);box-shadow:0 6px 18px 0 rgba(0,0,0,.06);">
                                            <tr>
                                                <td style="height:40px;">&nbsp;</td>
                                            </tr>
                                            <tr>
                                                <td style="padding:0 35px;">
                                                    <h3
                                                        style="color:#1e1e2d; font-weight:500; margin:0;font-size:32px;font-family:'Rubik',sans-serif;">
                                                        Budgeting System Account Activation</h3>
                                                    <span
                                                        style="display:inline-block; vertical-align:middle; margin:29px 0 26px; border-bottom:1px solid #cecece; width:100px;"></span>
                                                    <p style="color:#455056; font-size:15px;line-height:24px; margin:0;">
                                                        A Claxon Business Solution Budgeting system account have been created for
                                                        you. Please click the link below to activate and sign into your
                                                        account. Contact info@claxonactuaries.com for further information<br><br>
                                                  
                                                    </p>

                                                       <table border="0" cellpadding="0" cellspacing="0"      align="center"
                        style="margin-top: 8px">
                        <tr tyle="padding: 0; ">
                          <td
                            style="
                              padding: 8px;
                              background-color: #cba06c;
                              cursor: pointer;
                              border-radius: 4px;
                              color: #1e1e2d;
                              text-decoration: none;
                              font-size: 12px;
                              font-weight: bold;
                              text-transform: capitalize;
                              line-height: 1;
                              text-align: center;
                            "
                          >
                            <a
                              href="{url}"
                              target="_blank"
                              style="
                                display: inline-block;
                                color: #1e1e2d;
                                text-decoration: none;
                                font-size: 14px;
                                font-weight: bold;
                                text-transform: capitalize;
                              "
                              title="Reset Password"
                            >
                              RESET PASSWORD
                            </a>

                            
                          </td>
                        </tr>
                      </table>
                                                  
                                                    <b>


                 
                                                </td>
                                            </tr>
                                            <tr>
                                                <td style="height:40px;">
                                                    <img height="160" width="160" src="data:image/png;base64, {qrcode_image}"
                                                        alt="QR Code" />
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                                <tr>
                                    <td style="height:20px;">&nbsp;</td>
                                </tr>
                                <tr>
                                    <td style="text-align:center;">
                                    </td>
                                </tr>
                                <tr>
                                    <td style="height:80px;">&nbsp;</td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                </table>
            </body>

            </html>"""
    return html_body


def email_to_change_password(url: str):
    html_message = f"""<!doctype html>
        <html lang="en-US">

        <head>
            <meta content="text/html; charset=utf-8" http-equiv="Content-Type" />
            <title>Reset Password Email Template</title>
            <meta name="description" content="Reset Password Email Template.">
            <style type="text/css">

            </style>
        </head>

        <body marginheight="0" topmargin="0" marginwidth="0" style="margin: 0px; background-color: #f2f3f8;" leftmargin="0">
            <table cellspacing="0" border="0" cellpadding="0" width="100%" bgcolor="#f2f3f8"
                style="@import url(https://fonts.googleapis.com/css?family=Rubik:300,400,500,700|Open+Sans:300,400,600,700); font-family: 'Open Sans', sans-serif;">
                <tr>
                    <td>
                        <table style="background-color: #f2f3f8; max-width:670px;  margin:0 auto;" width="100%" border="0"
                            align="center" cellpadding="0" cellspacing="0">
                            <tr>
                                <td style="height:80px;">&nbsp;</td>
                            </tr>
                        
                            <tr>
                                <td style="height:20px;">&nbsp;</td>
                            </tr>
                            <tr>
                                <td>
                                    <table width="95%" border="0" align="center" cellpadding="0" cellspacing="0"
                                        style="max-width:670px;background:#fff; border-radius:3px; text-align:center;-webkit-box-shadow:0 6px 18px 0 rgba(0,0,0,.06);-moz-box-shadow:0 6px 18px 0 rgba(0,0,0,.06);box-shadow:0 6px 18px 0 rgba(0,0,0,.06);">
                                        <tr>
                                            <td style="height:40px;">&nbsp;</td>
                                        </tr>
                                        <tr>
                                            <td style="padding:0 35px;">
                                                <h1 style="color:#1e1e2d; font-weight:500; margin:0;font-size:32px;font-family:'Rubik',sans-serif;">You have
                                                    requested to reset your password</h1>
                                                <span
                                                    style="display:inline-block; vertical-align:middle; margin:29px 0 26px; border-bottom:1px solid #cecece; width:100px;"></span>
                                                <p style="color:#455056; font-size:15px;line-height:24px; margin:0;">
                                                    We cannot simply send you your old password. A unique link to reset your
                                                    password has been generated for you. To reset your password, click the
                                                    following link and follow the instructions.


                                                </p>
                                           

                     <table border="0" cellpadding="0" cellspacing="0"      align="center"
                        style="margin-top: 8px">
                        <tr tyle="padding: 0; ">
                          <td
                            style="
                              padding: 8px;
                              background-color: #cba06c;
                              cursor: pointer;
                              border-radius: 4px;
                              color: #1e1e2d;
                              text-decoration: none;
                              font-size: 12px;
                              font-weight: bold;
                              text-transform: capitalize;
                              line-height: 1;
                              text-align: center;
                            "
                          >
                            <a
                              href="{url}"
                              target="_blank"
                              style="
                                display: inline-block;
                                color: #1e1e2d;
                                text-decoration: none;
                                font-size: 14px;
                                font-weight: bold;
                                text-transform: capitalize;
                              "
                              title="Reset Password"
                            >
                              RESET PASSWORD
                            </a>

                            
                          </td>
                        </tr>
                      </table>
                                            </td>
                                        </tr>
                                        <tr>
                                            <td style="height:40px;">&nbsp;</td>
                                        </tr>
                                    </table>
                                </td>
                            <tr>
                                <td style="height:20px;">&nbsp;</td>
                            </tr>
                            <tr>
                                <td style="text-align:center;">
                                </td>
                            </tr>
                            <tr>
                                <td style="height:80px;">&nbsp;</td>
                            </tr>
                        </table>
                    </td>
                </tr>
            </table>
        </body>

        </html>"""

    return html_message
