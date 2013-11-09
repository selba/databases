Imports System.Windows.Forms
Namespace Access
    ''' <summary>
    ''' DBMS: Data Bades Management Sytem
    ''' Sistema Gestor de Bases de Datos
    ''' Controla el Acceso sobre una BD de Access
    ''' </summary>
    ''' <remarks></remarks>
    Public Class DBMS
#Region "ESTRUCTURAS"
        Enum ACCESS_TABLE_TYPE
            READ_ONLY = 1
            ADD_ONLY = 2
            READ_WRITE = 4
        End Enum
        Private Enum SQL_COMMAND_TYPE
            SQL_SELECT = 0
            SQL_INSERT = 1
            SQL_UPDATE = 2
            SQL_DELETE = 3
        End Enum
#End Region
#Region "VARIABLES"
        Private _Cn As New OleDb.OleDbConnection
        Private _DbLocation As String
        Private _DbName As String
        Private _DataAdapters As New Generic.Dictionary(Of String, OleDb.OleDbDataAdapter)
        Private _PopUpErrors As Boolean
        Private _DBFFiles As Boolean
        Private _OleDBProvider As String
#End Region
#Region "PROPIEDADES"
        ''' <summary>
        ''' Establece/Recupera el Path donde se encuentra la BD
        ''' </summary>
        ''' <value></value>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Property DbLocation() As String
            Get
                Return (_DbLocation)
            End Get
            Set(ByVal value As String)
                _DbLocation = value
            End Set
        End Property
        ''' <summary>
        ''' Establece/Recupera el nombre de la base de datos activa
        ''' </summary>
        ''' <value></value>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Property DbName() As String
            Get
                Return (_DbName)
            End Get
            Set(ByVal value As String)
                _DbName = value
            End Set
        End Property
        ''' <summary>
        ''' Si se especifica substituye al de defecto Microsoft.Jet.OLEDB.4.0
        ''' </summary>
        ''' <value></value>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Property OleDBProvider() As String
            Get
                Return _OleDBProvider
            End Get
            Set(ByVal value As String)
                _OleDBProvider = value
            End Set
        End Property
        Public ReadOnly Property GetConnection() As OleDb.OleDbConnection
            Get
                Dim OleDbPro As String = "Microsoft.Jet.OLEDB.4.0"
                If OleDBProvider <> "" Then
                    OleDbPro = OleDBProvider '"Microsoft.ACE.OLEDB.12.0"
                End If
                If Not _DBFFiles Then
                    Dim Cn As New OleDb.OleDbConnection
                    Cn.ConnectionString = "Provider=" + OleDbPro + "; Data Source=" & _DbLocation & "\" & _DbName & "; User Id=admin; Password="
                    Return (Cn)
                Else
                    Dim Cn As New OleDb.OleDbConnection
                    Cn.ConnectionString = "Provider=" + OleDbPro + "; Data Source=" & _DbLocation & ";Extended Properties=dBASE IV; User Id=admin; Password="
                    Return (Cn)
                End If
            End Get
        End Property
        ''' <summary>
        ''' Indica si pot mostrar messagebox quan detecti errors
        ''' </summary>
        ''' <value></value>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Property PopupErrors() As Boolean
            Get
                Return _PopUpErrors
            End Get
            Set(ByVal value As Boolean)
                _PopUpErrors = value
            End Set
        End Property
        ''' <summary>
        ''' Si TRUE aleshores la BD esperada es de tipus DBF (DBase IV)
        ''' </summary>
        ''' <value></value>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Property ExtendsToDBF() As Boolean
            Get
                Return _DBFFiles
            End Get
            Set(ByVal value As Boolean)
                _DBFFiles = value
            End Set
        End Property
#End Region
#Region "METODOS PUBLICOS"
        ''' <summary>
        ''' Obtiene un DataReader a partir de un comando SQL de tipo SELECT
        ''' </summary>
        ''' <param name="Sql"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function GetDataReader(ByVal Sql As String) As OleDb.OleDbDataReader
            Dim Cmd As New OleDb.OleDbCommand
            Dim Dr As OleDb.OleDbDataReader

            Cmd.Connection = GetConnection
            Cmd.CommandText = Sql
            Cmd.Connection.Open()
            Try
                Dr = Cmd.ExecuteReader(CommandBehavior.CloseConnection)
                Return (Dr)
            Catch e As Exception
                'Throw e
                If PopupErrors Then
                    MessageBox.Show("[DB]Error en la ejecución del comando SQL: " + vbCrLf + Sql + " " + e.Message)
                End If
                Return (Nothing)
                Exit Function
            End Try
        End Function
        ''' <summary>
        ''' Ejecuta un comando SQL de Acción
        ''' </summary>
        ''' <param name="Sql"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function DoSQLAction(ByVal Sql As String) As Int32
            Dim Cmd As New OleDb.OleDbCommand
            Dim AffectedRows As Int32

            Cmd.Connection = GetConnection
            Cmd.CommandText = Sql
            Cmd.Connection.Open()
            Try
                AffectedRows = Cmd.ExecuteNonQuery()
                Cmd.Connection.Close()
                Return (AffectedRows)
            Catch e As Exception
                If PopupErrors Then
                    MessageBox.Show("[DB]Error en la ejecución del comando SQL: " + vbCrLf + Sql + " " + e.Message)
                End If
                Cmd.Connection.Close()
                Return (0)
                Exit Function
            End Try
        End Function

        ''' <summary>
        ''' Devuelve una Tabla
        ''' Si se solicita de read/write crea (Si no existe) el DataAdapter correspondiente        ''' 
        ''' </summary>
        ''' <param name="TableName">Nombre de la Tabla o sentencia SQL si acceso READ_ONLY_USING_SQL</param>
        ''' <param name="Mode">Tipo de Acceso</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function GetTable(ByVal TableName As String, ByVal Mode As ACCESS_TABLE_TYPE) As DataTable
            Dim Dr As OleDb.OleDbDataReader
            Dim Table As New DataTable
            Dim StructTable As DataTable
            Dim Adaptador As OleDb.OleDbDataAdapter

            Select Case Mode
                Case ACCESS_TABLE_TYPE.ADD_ONLY
                    Dr = GetDataReader("Select top 1 * from " & TableName)
                Case Else
                    Dr = GetDataReader("Select * from " & TableName)
            End Select

            Table.Load(Dr)
            Table.TableName = TableName


            'Si es lectura escritura debe crear el correspondiente dataadapter
            If Mode = ACCESS_TABLE_TYPE.READ_WRITE Or Mode = ACCESS_TABLE_TYPE.ADD_ONLY Then
                Try
                    Adaptador = _DataAdapters(TableName) 'Comprueba si existe

                Catch ex As KeyNotFoundException
                    StructTable = GetSchemaTable(TableName)
                    Adaptador = New OleDb.OleDbDataAdapter("Select * from " & TableName, _Cn.ConnectionString)
                    Adaptador.InsertCommand = DoSqlCommand(Table, SQL_COMMAND_TYPE.SQL_INSERT, StructTable)
                    Adaptador.UpdateCommand = DoSqlCommand(Table, SQL_COMMAND_TYPE.SQL_UPDATE, StructTable)
                    Adaptador.DeleteCommand = DoSqlCommand(Table, SQL_COMMAND_TYPE.SQL_DELETE, StructTable)
                    _DataAdapters.Add(TableName, Adaptador)
                End Try
            End If

            Return (Table)
        End Function
        Public Function UpdateTable(ByVal Table As DataTable) As DataTable
            Dim Adaptador As OleDb.OleDbDataAdapter
            Dim ChangesTable As DataTable
            Try
                Adaptador = _DataAdapters(Table.TableName) 'Comprueba si existe
                ChangesTable = Table.GetChanges()
                If ChangesTable IsNot Nothing Then
                    Adaptador.Update(ChangesTable)
                End If
                Table.AcceptChanges()
                Return (Table)
            Catch ex As KeyNotFoundException
                If PopupErrors Then
                    MessageBox.Show("[DB]Error Actualizando tabla: " + ex.Message)
                End If
                Return (Nothing)
            Catch ex1 As Exception
                If PopupErrors Then
                    MessageBox.Show("[DB]Error Actualizando tabla: " + ex1.Message)
                End If
                Return (Nothing)
            End Try
        End Function

        ''' <summary>
        ''' Devuelve una Tabla a partir de una sentencia SQL
        ''' </summary>
        ''' <param name="SqlText">Sentencia SQL</param>
        ''' <param name="TableName">Nombre de la Tabla de Retorno</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function GetSqlTable(ByVal SqlText As String, ByVal TableName As String) As DataTable
            Dim Dr As OleDb.OleDbDataReader = Nothing
            Dim Table As New DataTable
            Try
                Dr = GetDataReader(SqlText)
            Catch e As Exception
                If PopupErrors Then
                    MessageBox.Show("[DB]Error: " + e.Message)
                End If
                'Throw e
            End Try
            Table.Load(Dr)
            Table.TableName = TableName
            Return (Table)
        End Function

        Public Function CreateAdapter(ByVal TableName As String) As OleDb.OleDbDataAdapter
            Dim Dr As OleDb.OleDbDataReader
            Dim Table As New DataTable
            Dim StructTable As DataTable
            Dim Adaptador As OleDb.OleDbDataAdapter

            Dr = GetDataReader("Select top 1 * from " & TableName)
            Table.Load(Dr)
            Table.TableName = TableName

            Try
                Adaptador = _DataAdapters(TableName) 'Comprueba si existe

            Catch ex As KeyNotFoundException
                StructTable = GetSchemaTable(TableName)
                Adaptador = New OleDb.OleDbDataAdapter("Select * from " & TableName, _Cn.ConnectionString)
                Adaptador.InsertCommand = DoSqlCommand(Table, SQL_COMMAND_TYPE.SQL_INSERT, StructTable)
                Adaptador.UpdateCommand = DoSqlCommand(Table, SQL_COMMAND_TYPE.SQL_UPDATE, StructTable)
                Adaptador.DeleteCommand = DoSqlCommand(Table, SQL_COMMAND_TYPE.SQL_DELETE, StructTable)
                _DataAdapters.Add(TableName, Adaptador)
            End Try
            Return Adaptador
        End Function
        Public Function GetSchemaDB() As DataTable
            Dim Cn As OleDb.OleDbConnection = GetConnection
            Cn.Open()
            Dim Table As DataTable = Cn.GetSchema("Tables", New String() {Nothing, Nothing, Nothing, "Table"})
            Cn.Close()
            Return Table
        End Function
#End Region
#Region "METODOS PRIVADOS"
        Private Function GetSchemaTable(ByVal TableName As String) As DataTable
            Dim Cmd As New OleDb.OleDbCommand
            Dim Dr As OleDb.OleDbDataReader
            Dim Schema As DataTable

            Cmd.Connection = GetConnection
            Cmd.CommandText = "Select * from " & TableName
            Cmd.Connection.Open()

            Try
                Dr = Cmd.ExecuteReader(CommandBehavior.KeyInfo)
                Schema = Dr.GetSchemaTable
                Cmd.Connection.Close()
                Return (Schema)
            Catch e As Exception
                Return (Nothing)
                Exit Function
            End Try
        End Function

        Private Function DoSqlCommand(ByVal Table As DataTable, ByVal SqlType As SQL_COMMAND_TYPE, ByVal StructTable As DataTable) As OleDb.OleDbCommand
            Dim Command As New OleDb.OleDbCommand
            Dim i As Int32, FirstField As Boolean

            Command.CommandType = CommandType.Text
            Command.Connection = GetConnection
            FirstField = True
            Select Case SqlType
                Case SQL_COMMAND_TYPE.SQL_SELECT
                    Command.CommandText = "Select * from " & Table.TableName
                Case SQL_COMMAND_TYPE.SQL_INSERT
                    Command.CommandText = "Insert into " & Table.TableName & "("
                    For i = 0 To Table.Columns.Count - 1
                        If CType(StructTable.Rows(i).Item("IsAutoIncrement"), Boolean) = False Then
                            If FirstField Then
                                FirstField = False
                                Command.CommandText &= "[" + Table.Columns(i).ColumnName + "]"
                            Else
                                Command.CommandText &= "," & "[" + Table.Columns(i).ColumnName + "]"
                            End If
                            Dim Parameter As New OleDb.OleDbParameter
                            Parameter.ParameterName = "p" & Table.Columns(i).ColumnName
                            Parameter.SourceColumn = Table.Columns(i).ColumnName
                            Command.Parameters.Add(Parameter)
                        End If
                    Next
                    Command.CommandText &= ")values("
                    FirstField = True
                    For i = 0 To Table.Columns.Count - 1
                        If CType(StructTable.Rows(i).Item("IsAutoIncrement"), Boolean) = False Then
                            If FirstField Then
                                FirstField = False
                                Command.CommandText &= "[p" & Table.Columns(i).ColumnName & "]"
                            Else
                                Command.CommandText &= ",[p" & Table.Columns(i).ColumnName & "]"
                            End If
                        End If
                    Next
                    Command.CommandText &= ")"
                Case SQL_COMMAND_TYPE.SQL_UPDATE
                    Command.CommandText = "Update " & Table.TableName & " Set "
                    For i = 0 To Table.Columns.Count - 1
                        If CType(StructTable.Rows(i).Item("Iskey"), Boolean) = False And _
                        CType(StructTable.Rows(i).Item("IsAutoIncrement"), Boolean) = False Then
                            If FirstField Then
                                FirstField = False
                                Command.CommandText &= "[" + Table.Columns(i).ColumnName + "]" & "=[p" & Table.Columns(i).ColumnName & "]"
                            Else
                                Command.CommandText &= "," & "[" + Table.Columns(i).ColumnName + "]" & "=[p" & Table.Columns(i).ColumnName & "]"
                            End If
                            Dim Parameter As New OleDb.OleDbParameter
                            Parameter.ParameterName = "p" & Table.Columns(i).ColumnName
                            Parameter.SourceColumn = Table.Columns(i).ColumnName
                            Command.Parameters.Add(Parameter)
                        End If
                    Next
                    Command.CommandText &= " where "
                    FirstField = True
                    For i = 0 To Table.Columns.Count - 1
                        If CType(StructTable.Rows(i).Item("IsKey"), Boolean) = True Then
                            If FirstField Then
                                FirstField = False
                                Command.CommandText &= Table.Columns(i).ColumnName & "=[p" & Table.Columns(i).ColumnName & "]"
                            Else
                                Command.CommandText &= " and " & Table.Columns(i).ColumnName & "=[p" & Table.Columns(i).ColumnName & "]"
                            End If
                            Dim Parameter As New OleDb.OleDbParameter
                            Parameter.ParameterName = "p" & Table.Columns(i).ColumnName
                            Parameter.SourceColumn = Table.Columns(i).ColumnName
                            Command.Parameters.Add(Parameter)
                        End If
                    Next
                Case SQL_COMMAND_TYPE.SQL_DELETE
                    Command.CommandText = "Delete * from " & Table.TableName & " where "
                    For i = 0 To Table.Columns.Count - 1
                        If CType(StructTable.Rows(i).Item("IsKey"), Boolean) = True Then
                            If FirstField Then
                                FirstField = False
                                Command.CommandText &= "[" + Table.Columns(i).ColumnName + "]" & "=[p" & Table.Columns(i).ColumnName & "]"
                            Else
                                Command.CommandText &= " and " & "[" + Table.Columns(i).ColumnName + "]" & "=[p" & Table.Columns(i).ColumnName & "]"
                            End If
                            Dim Parameter As New OleDb.OleDbParameter
                            Parameter.ParameterName = "p" & Table.Columns(i).ColumnName
                            Parameter.SourceColumn = Table.Columns(i).ColumnName
                            Command.Parameters.Add(Parameter)
                        End If
                    Next
            End Select
            Return Command
        End Function
#End Region
    End Class
End Namespace
