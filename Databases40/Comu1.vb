Public Module Comu1

    ''' <summary>Devuelve la versi�n del m�dulo solictado</summary>
    ''' <returns>La versi�n de la librer�a</returns>
    Public Function GetLibraryVersion() As String
        GetLibraryVersion = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString()
    End Function

End Module
